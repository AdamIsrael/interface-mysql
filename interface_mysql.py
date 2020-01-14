from collections import namedtuple

from ops.framework import EventBase, EventsBase, EventSource, Object, StoredState
from ops.model import BlockedStatus, WaitingStatus


class DatabaseAvailableEvent(EventBase):
    pass


class DatabaseChangedEvent(EventBase):
    pass


class MySQLServerEvents(EventsBase):
    database_available = EventSource(DatabaseAvailableEvent)
    database_changed = EventSource(DatabaseChangedEvent)


class MySQLServer(Object):
    on = MySQLServerEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.relation_name = relation_name

        self.state.database = getattr(self.state, 'database', {})

        self.framework.observe(charm.on[relation_name].relation_changed, self.on_changed)
        self.framework.observe(charm.on[relation_name].relation_broken, self.on_broken)

    def on_changed(self, event):
        try:
            db = self.database()
        except MySQLServerError:
            pass
        else:
            if db.host:
                prev_db = self.state.database
                # Subclasses of dict can't be marshalled so can't be stored directly.
                self.state.database = dict(db)
                if not prev_db:
                    self.on.database_available.emit()
                elif db != prev_db:
                    self.on.database_changed.emit()

    def on_broken(self, event):
        self.state.database = {}

    @property
    def _relations(self):
        return self.framework.model.relations[self.relation_name]

    def database(self):
        if not self._relations:
            raise MissingRelationError(self.relation_name)
        if len(self._relations) > 1:
            raise TooManyDatabasesError(self.relation_name, len(self._relations))
        db = MySQLDatabase(self._relations[0])
        if not db.host:
            raise IncompleteDatabaseError(self.relation_name)
        return db

    def databases(self):
        if not self._relations:
            raise MissingRelationError(self.relation_name)
        dbs = []
        for relation in self._relations:
            db = MySQLDatabase(relation)
            if db.host:
                dbs.append(db)
        if not dbs:
            raise IncompleteDatabaseError(self.relation_name)
        return dbs


class MySQLDatabase(dict):
    def __init__(self, relation):
        # Prefer the app data, but older charms will have info set by the leader.
        for candidate in [relation.app] + list(relation.units):
            data = relation.data[candidate]
            if all(data.get(field) for field in ('database', 'host', 'user', 'password')):
                super().__init__(
                    name=data['database'],
                    host=data['host'],
                    port=data.get('port', '3306'),
                    username=data['user'],
                    password=data['password'],
                )
                break
        else:
            super().__init__(
                name=None,
                host=None,
                port=None,
                username=None,
                password=None,
            )

    @property
    def name(self):
        return self['name']

    @property
    def host(self):
        return self['host']

    @property
    def port(self):
        return self['port']

    @property
    def username(self):
        return self['username']

    @property
    def password(self):
        return self['password']


class MySQLServerError(Exception):
    # Status will be set by subclass.
    status = None


class MissingRelationError(MySQLServerError):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.status = BlockedStatus(f'Missing relation: {relation_name}')


class TooManyDatabasesError(MySQLServerError):
    def __init__(self, relation_name, num_apps):
        super().__init__(relation_name)
        self.status = BlockedStatus(f'Too many related applications: {relation_name}')


class IncompleteDatabaseError(MySQLServerError):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.status = WaitingStatus(f'Waiting for database: {relation_name}')
