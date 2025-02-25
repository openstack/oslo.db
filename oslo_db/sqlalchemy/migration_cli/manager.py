#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from debtcollector import removals
import sqlalchemy
from stevedore import enabled

from oslo_db import exception


MIGRATION_NAMESPACE = 'oslo.db.migration'


def check_plugin_enabled(ext):
    """Used for EnabledExtensionManager."""
    return ext.obj.enabled


@removals.remove(
    message='Support for sqlalchemy-migrate and with it the migration manager '
    'is deprecated for removal; consider migrating to and using alembic '
    'directly',
    version='8.3.0'
)
class MigrationManager:

    def __init__(self, migration_config, engine=None):
        if engine is None:
            if migration_config.get('db_url'):
                engine = sqlalchemy.create_engine(
                    migration_config['db_url'],
                    poolclass=sqlalchemy.pool.NullPool,
                )
            else:
                raise ValueError('Either database url or engine'
                                 ' must be provided.')

        self._manager = enabled.EnabledExtensionManager(
            MIGRATION_NAMESPACE,
            check_plugin_enabled,
            invoke_args=(engine, migration_config),
            invoke_on_load=True
        )
        if not self._plugins:
            raise ValueError('There must be at least one plugin active.')

    @property
    def _plugins(self):
        return sorted(ext.obj for ext in self._manager.extensions)

    def upgrade(self, revision):
        """Upgrade database with all available backends."""
        # a revision exists only in a single plugin. Until we reached it, we
        # should upgrade to the plugins' heads.
        # revision=None is a special case meaning latest revision.
        rev_in_plugins = [p.has_revision(revision) for p in self._plugins]
        if not any(rev_in_plugins) and revision is not None:
            raise exception.DBMigrationError('Revision does not exist')

        results = []
        for plugin, has_revision in zip(self._plugins, rev_in_plugins):
            if not has_revision or revision is None:
                results.append(plugin.upgrade(None))
            else:
                results.append(plugin.upgrade(revision))
                break
        return results

    def downgrade(self, revision):
        """Downgrade database with available backends."""
        # a revision exists only in a single plugin. Until we reached it, we
        # should upgrade to the plugins' first revision.
        # revision=None is a special case meaning initial revision.
        rev_in_plugins = [p.has_revision(revision) for p in self._plugins]
        if not any(rev_in_plugins) and revision is not None:
            raise exception.DBMigrationError('Revision does not exist')

        # downgrading should be performed in reversed order
        results = []
        for plugin, has_revision in zip(reversed(self._plugins),
                                        reversed(rev_in_plugins)):
            if not has_revision or revision is None:
                results.append(plugin.downgrade(None))
            else:
                results.append(plugin.downgrade(revision))
                break
        return results

    def version(self):
        """Return last version of db."""
        last = None
        for plugin in self._plugins:
            version = plugin.version()
            if version is not None:
                last = version
        return last

    def revision(self, message, autogenerate):
        """Generate template or autogenerated revision."""
        # revision should be done only by last plugin
        return self._plugins[-1].revision(message, autogenerate)

    def stamp(self, revision):
        """Create stamp for a given revision."""
        return self._plugins[-1].stamp(revision)
