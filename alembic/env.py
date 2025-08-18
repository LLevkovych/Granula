import os
from logging.config import fileConfig
from urllib.parse import urlparse

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.db.models import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Allow DATABASE_URL env override
env_url = os.getenv("DATABASE_URL")
if env_url:
	config.set_main_option("sqlalchemy.url", env_url)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
	fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata


def is_async_url(url: str) -> bool:
	if not url:
		return False
	scheme = urlparse(url).scheme
	return "+" in scheme  # e.g., sqlite+aiosqlite, postgresql+asyncpg


def run_migrations_offline() -> None:
	"""Run migrations in 'offline' mode."""
	url = config.get_main_option("sqlalchemy.url")
	context.configure(
		url=url,
		target_metadata=target_metadata,
		literal_binds=True,
		dialect_opts={"paramstyle": "named"},
	)

	with context.begin_transaction():
		context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
	context.configure(connection=connection, target_metadata=target_metadata)
	with context.begin_transaction():
		context.run_migrations()


async def run_migrations_online_async() -> None:
	connectable = async_engine_from_config(
		config.get_section(config.config_ini_section, {}),
		prefix="sqlalchemy.",
		poolclass=pool.NullPool,
	)
	async with connectable.connect() as connection:
		await connection.run_sync(do_run_migrations)


def run_migrations_online_sync() -> None:
	connectable = engine_from_config(
		config.get_section(config.config_ini_section, {}),
		prefix="sqlalchemy.",
		poolclass=pool.NullPool,
	)
	with connectable.connect() as connection:
		do_run_migrations(connection)


if context.is_offline_mode():
	run_migrations_offline()
else:
	url = config.get_main_option("sqlalchemy.url")
	if is_async_url(url):
		import asyncio
		asyncio.run(run_migrations_online_async())
	else:
		run_migrations_online_sync()
