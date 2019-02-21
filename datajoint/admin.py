import pymysql
from hashlib import sha1
from getpass import getpass
from .connection import conn
from .settings import config
from .utils import user_choice


def set_password(new_password=None, connection=None, update_config=None):   # pragma: no cover
    connection = conn() if connection is None else connection
    if new_password is None:
        new_password = getpass('New password: ')
        confirm_password = getpass('Confirm password: ')
        if new_password != confirm_password:
            print('Failed to confirm the password! Aborting password change.')
            return
    # hash on client to increase PROCESS privelege security on dj-only db's.
    new_hash = sha1(new_password.encode()).hexdigest()
    new_hash = connection.query("SELECT CONCAT('*', UPPER(SHA1(UNHEX('%s'))))" % new_hash).fetchall()[0]
    # results in doubly hashed value..
    connection.query("SET PASSWORD = '%s'" % new_hash)
    # need to update mysql.user .... but this requires privs maybe.. so meh
    # and also drifts mysql.user.password mysql.user.authentication_string 
    # so would need to also select version();
    print('Password updated.')

    if update_config or (update_config is None and user_choice('Update local setting?') == 'yes'):
        config['database.password'] = new_password
        config.save_local(verbose=True)


def kill(restriction=None, connection=None):  # pragma: no cover
    """
    view and kill database connections.
    :param restriction: restriction to be applied to processlist
    :param connection: a datajoint.Connection object. Default calls datajoint.conn()

    Restrictions are specified as strings and can involve any of the attributes of
    information_schema.processlist: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.

    Examples:
        dj.kill('HOST LIKE "%compute%"') lists only connections from hosts containing "compute".
        dj.kill('TIME > 600') lists only connections older than 10 minutes.
    """

    if connection is None:
        connection = conn()

    query = 'SELECT * FROM information_schema.processlist WHERE id <> CONNECTION_ID()' + (
        "" if restriction is None else ' AND (%s)' % restriction)

    while True:
        print('  ID USER         STATE         TIME  INFO')
        print('+--+ +----------+ +-----------+ +--+')
        cur = connection.query(query, as_dict=True)
        for process in cur:
            try:
                print('{ID:>4d} {USER:<12s} {STATE:<12s} {TIME:>5d}  {INFO}'.format(**process))
            except TypeError:
                print(process)
        response = input('process to kill or "q" to quit > ')
        if response == 'q':
            break
        if response:
            try:
                pid = int(response)
            except ValueError:
                pass  # ignore non-numeric input
            else:
                try:
                    connection.query('kill %d' % pid)
                except pymysql.err.InternalError:
                    print('Process not found')
