from ops.framework import EventBase, EventsBase, EventSource, Object, StoredState
from ops.model import BlockedStatus, ModelError, WaitingStatus


class DatabaseError(ModelError):
    status_type = BlockedStatus
    status_message = 'Database error'

    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.status = self.status_type(f'{self.status_message}: {relation_name}')


class IncompleteRelationError(DatabaseError):
    status_type = WaitingStatus
    status_message = 'Waiting for relation'


class NoRelatedAppsError(DatabaseError):
    status_message = 'Missing relation'


class TooManyRelatedAppsError(DatabaseError):
    status_message = 'Too many related applications'


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


class DatabaseLostEvent(EventBase):
    def __init__(self, handle, relation_name):
        super().__init__(handle)
        self.relation_name = relation_name
        self.status = NoRelatedAppsError(relation_name).status

    def snapshot(self):
        return self.relation_name

    def restore(self, relation_name):
        self.relation_name = relation_name
        self.status = NoRelatedAppsError(relation_name).status


class MySQLClientEvents(EventsBase):
    database_available = EventSource(DatabaseAvailableEvent)
    database_changed = EventSource(DatabaseChangedEvent)
    database_lost = EventSource(DatabaseLostEvent)


class MySQLClient(Object):
    on = MySQLClientEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.state.set_default(db_hash=None)
        self.relation_name = relation_name

        self.framework.observe(charm.on[relation_name].relation_changed, self.on_changed)
        self.framework.observe(charm.on[relation_name].relation_broken, self.on_broken)

    def on_changed(self, event):
        try:
            db = self.database()
        except ModelError as e:
            had_db = self.state.db_hash is not None
            self.state.db_hash = None
            if had_db:
                self.on.database_lost.emit(e.status)
        else:
            old_hash = self.state.db_hash
            new_hash = self.state.db_hash = hash(frozenset(db.items()))
            if old_hash is None:
                self.on.database_available.emit(db)
            elif new_hash != old_hash:
                self.on.database_changed.emit(db)

    def on_broken(self, event):
        self.state.db_hash = None
        self.on.database_lost.emit(event.relation.name)

    @property
    def _relations(self):
        return self.framework.model.relations[self.relation_name]

    def database(self):
        if not self._relations:
            raise NoRelatedAppsError(self.relation_name)
        if len(self._relations) > 1:
            raise TooManyRelatedAppsError(self.relation_name)
        db = MySQLDatabase.from_relation(self._relations[0])
        if db is None:
            raise IncompleteRelationError(self.relation_name)
        return db


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
