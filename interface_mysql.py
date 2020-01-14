from collections import namedtuple

from ops.framework import EventBase, EventsBase, EventSource, Object, StoredState
from ops.model import BlockedStatus, StatusBase, WaitingStatus


class DatabaseEvent(EventBase):
    def __init__(self, handle, database):
        super().__init__(handle)
        self.database = database

    def snapshot(self):
        return dict(self.database)

    def restore(self, snapshot):
        self.database = MySQLDatabase(snapshot)


class DatabaseAvailableEvent(DatabaseEvent):
    pass


class DatabaseChangedEvent(DatabaseEvent):
    pass


class DatabaseUnavailableEvent(EventBase):
    def __init__(self, handle, status):
        super().__init__(handle)
        self.status = status

    def snapshot(self):
        return {
            'name': self.status.name,
            'message': self.status.message,
        }

    def restore(self, snapshot):
        self.status = StatusBase.from_name(snapshot['name'], snapshot['message'])


class MySQLServerEvents(EventsBase):
    database_available = EventSource(DatabaseAvailableEvent)
    database_changed = EventSource(DatabaseChangedEvent)
    database_unavailable = EventSource(DatabaseUnavailableEvent)


class MySQLServer(Object):
    on = MySQLServerEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.relation_name = relation_name

        self.framework.observe(charm.on.start, self.init_state)
        self.framework.observe(charm.on[relation_name].relation_changed, self.on_changed)
        self.framework.observe(charm.on[relation_name].relation_broken, self.on_broken)

    def init_state(self, event):
        self.state.database = {}
        # Force checking for the database, even though it's almost certainly not there yet,
        # just so we can at least inform the charm of that fact.
        self.on_changed(event)

    def on_changed(self, event):
        try:
            db = self.database()
        except MySQLServerError as e:
            self.state.database = {}
            self.on.database_unavailable.emit(e.status)
        else:
            prev_db = self.state.database
            # Subclasses of dict can't be marshalled so can't be stored directly.
            self.state.database = dict(db)
            if not prev_db:
                self.on.database_available.emit(db)
            elif db != prev_db:
                self.on.database_changed.emit(db)

    def on_broken(self, event):
        self.state.database = {}
        self.on.database_unavailable.emit(BlockedStatus(f'Missing relation: {self.relation_name}'))

    @property
    def _relations(self):
        return self.framework.model.relations[self.relation_name]

    def database(self):
        if not self._relations:
            raise MissingRelationError(self.relation_name)
        if len(self._relations) > 1:
            raise TooManyDatabasesError(self.relation_name, len(self._relations))
        db = MySQLDatabase.from_relation(self._relations[0])
        if db is None:
            raise IncompleteDatabaseError(self.relation_name)
        return db


# TODO: Maybe if this is an Object, it could be stored directly?
class MySQLDatabase(dict):
    @classmethod
    def from_relation(cls, relation):
        # Prefer the app data, but older charms will have info set by the leader.
        for candidate in [relation.app] + list(relation.units):
            data = relation.data[candidate]
            if all(data.get(field) for field in ('database', 'host', 'user', 'password')):
                return cls(
                    name=data['database'],
                    host=data['host'],
                    port=data.get('port', '3306'),
                    username=data['user'],
                    password=data['password'],
                )
        else:
            return None

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
