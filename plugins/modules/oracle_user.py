#!/usr/bin/python
# -*- coding: utf-8 -*-

import cx_Oracle
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.basic import os

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'}

DOCUMENTATION = '''
module: oracle_user
short_description: Manages Oracle user/schema.
description:
    - This module manages Oracle user/schema.
    - It can create, alter or drop users.
    - It can empty schemas (droping all its content).
    - It can change password of users ; lock/unlock and expire/unexpire accounts.
    - It can't be used to give privileges (refer to oracle_grant).
version_added: "1.9.1"
author:
    - Mikael Sandström (@oravirt)
    - Ari Stark (@ari-stark)
options:
    authentication_type:
        description:
            - Type of authentication for the user.
            - If not specified for a new user and no I(schema_password) is specified, there won't be authentication.
            - If not specified and I(schema_password) is specified, value will be forced to I(password).
        required: false
        default: None
        type: str
        choices: ['external', 'global', 'no_authentification', 'password']
    default_tablespace:
        description:
            - Default tablespace for the user.
            - Tablespace must exist.
            - If not specified for a new user, Oracle default will be used.
        required: false
        type: str
    expired:
        description:
            - Expire or unexpire account.
            - If not specified for a new user, Oracle default will be used.
        required: false
        type: bool
    hostname:
        description:
            - Specify the host name or IP address of the database server computer.
        default: localhost
        type: str
    locked:
        description:
            - Lock or unlock account.
            - If not specified for a new user, Oracle default will be used.
        required: false
        type: bool
    mode:
        description:
            - This option is the database administration privileges.
        default: normal
        type: str
        choices: ['normal', 'sysdba']
    oracle_home:
        description:
            - Define the directory into which all Oracle software is installed.
            - Define ORACLE_HOME environment variable if set.
        type: str
    password:
        description:
            - Set the password to use to connect the database server.
            - Must not be set if using Oracle wallet.
        type: str
    port:
        description:
            - Specify the listening port on the database server.
        default: 1521
        type: int
    profile:
        description:
            - Profile of the user.
            - Profile must exist.
            - If not specified for a new user, Oracle default will be used.
        required: false
        type: str
    schema_name:
        description:
            - Name of the user to manage.
        required: true
        type: str
        aliases:
            - name
    schema_password:
        description:
            - Password of the user account.
        required: true if I(authentication_type) is I(password).
        type: str
    service_name:
        description:
            - Specify the service name of the database you want to access.
        required: true
        type: str
    state:
        description:
            - Specify the state of the user/schema.
            - If I(state=empty), the schema will be purged, but not dropped.
            - If I(state=absent), the tablespace will be droped, including all datafiles.
        default: present
        type: str
        choices: ['absent', 'empty', 'present']
    temporary_tablespace:
        description:
            - Default temporary tablespace for the user.
            - Tablespace must exist.
            - If not specified for a new user, Oracle default will be used.
        required: false
        type: str
    username:
        description:
            - Set the login to use to connect the database server.
            - Must not be set if using Oracle wallet.
        type: str
        aliases:
            - user
requirements:
    - Python module cx_Oracle
    - Oracle basic tools.
notes:
    - Check mode and diff mode are supported.
    - Changes made by @ari-stark broke previous module interface.
'''

EXAMPLES = '''
# Create a new schema on a remote db by running the module on the controlmachine
oracle_user:
    hostname: "remote-db-server"
    service_name: "orcl"
    username: "system"
    password: "manager"
    schema_name: "myschema"
    schema_password: "mypass"
    default_tablespace: "test"
    state: "present"

# Drop a user on a remote db
oracle_user:
    hostname: "remote-db-server"
    service_name: "orcl"
    username: "system"
    password: "manager"
    schema_name: "myschema"
    state: "absent"

# Empty a schema on a remote db
oracle_user:
    hostname: "remote-db-server"
    service_name: "orcl"
    username: "system"
    password: "manager"
    schema_name: "myschema"
    state: "empty"
'''

RETURN = '''
ddls:
    description: Ordered list of DDL requests executed during module execution.
    returned: always
    type: list
    elements: str
'''

global module
global cursor
global ddls
global diff
global dsn


def execute_select(sql, params=None):
    """Executes a select query and return fetched data"""
    try:
        if params:
            return cursor.execute(sql, params).fetchall()
        else:
            return cursor.execute(sql).fetchall()
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code, request=sql)


def execute_ddl(request):
    """Execute a DDL request if not in check_mode"""
    try:
        if not module.check_mode:
            cursor.execute(request)
            ddls.append(request)
        else:
            ddls.append('--' + request)
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code, request=request, ddls=ddls)


def get_existing_user(schema_name):
    """Check if the user/schema exists"""
    data = execute_select('select username,'
                          '       account_status,'
                          '       default_tablespace,'
                          '       temporary_tablespace,'
                          '       profile,'
                          '       authentication_type,'
                          '       oracle_maintained'
                          '  from dba_users'
                          ' where username = upper(:schema_name)', {'schema_name': schema_name})
    if data:
        row = data[0]
        state = 'present'
        expired = 'EXPIRED' in row[1]
        locked = 'LOCKED' in row[1]
        default_tablespace = row[2]
        temporary_tablespace = row[3]
        profile = row[4]
        authentication_type = {'EXTERNAL': 'external', 'GLOBAL': 'global', 'NONE': None, 'PASSWORD': 'password'}[row[5]]
        oracle_maintained = row[6] == 'Y'

        diff['before']['state'] = state
        diff['before']['expired'] = expired
        diff['before']['locked'] = locked
        diff['before']['default_tablespace'] = default_tablespace
        diff['before']['temporary_tablespace'] = temporary_tablespace
        diff['before']['profile'] = profile
        diff['before']['authentication_type'] = authentication_type
        if authentication_type == 'password':
            diff['before']['schema_password'] = '**'

        return {'username': schema_name, 'state': state, 'expired': expired, 'locked': locked,
                'default_tablespace': default_tablespace, 'temporary_tablespace': temporary_tablespace,
                'profile': profile, 'authentication_type': authentication_type, 'oracle_maintained': oracle_maintained}

    else:
        diff['before']['state'] = 'absent'
        return None


# Check if password has changed
def has_password_changed(schema_name, schema_password):
    connection_parameters = {'user': schema_name, 'password': schema_password, 'dsn': dsn}
    # Connecting
    try:
        cx_Oracle.connect(**connection_parameters)
        return False
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        return error.code == 1017  # invalid username/password; logon denied


def ensure_present(schema_name, authentication_type, schema_password, default_tablespace, temporary_tablespace,
                   profile, locked, expired, empty):
    """Create or modify the user"""
    prev_user = get_existing_user(schema_name)

    if prev_user:
        changed = False
        emptied = False

        # Values are not changed by default, so after should be same as before
        diff['after']['authentication_type'] = diff['before']['authentication_type']
        diff['after']['default_tablespace'] = diff['before']['default_tablespace']
        diff['after']['expired'] = diff['before']['expired']
        diff['after']['locked'] = diff['before']['locked']
        diff['after']['profile'] = diff['before']['profile']
        diff['after']['temporary_tablespace'] = diff['before']['temporary_tablespace']

        sql = 'alter user %s ' % schema_name
        if authentication_type and authentication_type != prev_user['authentication_type']:
            if authentication_type == 'external':
                sql += 'identified externally '
            elif authentication_type == 'global':
                sql += 'identified globally '
            elif authentication_type == 'password':
                sql += 'identified by "%s" ' % schema_password
                diff['after']['schema_password'] = '*'
            else:
                sql += 'no authentication '
            diff['after']['authentication_type'] = authentication_type
            changed = True

        if default_tablespace and default_tablespace.lower() != prev_user['default_tablespace'].lower():
            sql += 'default tablespace %s quota unlimited on %s ' % (default_tablespace, default_tablespace)
            diff['after']['default_tablespace'] = default_tablespace
            changed = True

        if temporary_tablespace and temporary_tablespace.lower() != prev_user['temporary_tablespace'].lower():
            sql += 'temporary tablespace %s ' % temporary_tablespace
            diff['after']['temporary_tablespace'] = temporary_tablespace
            changed = True

        if profile and profile.lower() != prev_user['profile'].lower():
            sql += 'profile %s ' % profile
            diff['after']['profile'] = profile
            changed = True

        if locked is not None and locked != prev_user['locked']:
            sql += 'account %s ' % ('lock' if locked else 'unlock')
            diff['after']['locked'] = locked
            changed = True

        if expired is True and expired != prev_user['expired']:
            sql += 'password expire '
            diff['after']['expired'] = expired
            changed = True

        # If a password is defined and authentication type hasn't changed, we have to check :
        # - if account must be unexpire
        # - if password has changed
        if schema_password and authentication_type == prev_user['authentication_type']:
            # Unexpire account by defining a password
            if expired is False and expired != prev_user['expired']:
                sql += 'identified by "%s" ' % schema_password
                diff['after']['expired'] = expired
                diff['after']['password'] = '*'
                changed = True
            elif has_password_changed(schema_name, schema_password):
                sql += 'identified by "%s" ' % schema_password
                diff['after']['password'] = '*'
                changed = True

        if empty:
            rows = execute_select(
                "select object_name, object_type"
                "  from all_objects"
                " where object_type in('TABLE', 'VIEW', 'PACKAGE', 'PROCEDURE', 'FUNCTION', 'SEQUENCE',"
                "                      'SYNONYM', 'TYPE', 'DATABASE LINK', 'TABLE PARTITION')"
                "   and owner = '%s' and generated = 'N'" % schema_name.upper())

            for row in rows:
                object_name = row[0]
                object_type = row[1]
                execute_ddl('drop %s %s."%s" %s' % (
                    object_type, schema_name, object_name, 'cascade constraints' if object_type == 'TABLE' else ''))

            if len(rows) != 0:
                emptied = True

        if changed or emptied:
            if changed:
                execute_ddl(sql)
            module.exit_json(msg='User %s changed and/or schema emptied.' % schema_name, changed=True, diff=diff,
                             ddls=ddls)
        else:
            module.exit_json(msg='User %s already exists.' % schema_name, changed=False, diff=diff, ddls=ddls)
    else:
        sql = 'create user %s ' % schema_name
        if authentication_type == 'external':
            sql += 'identified externally '
        elif authentication_type == 'global':
            sql += 'identified globally '
        elif authentication_type == 'password':
            sql += 'identified by "%s" ' % schema_password
        else:
            sql += 'no authentication '
        if default_tablespace:
            sql += 'default tablespace %s quota unlimited on %s ' % (default_tablespace, default_tablespace)
        if temporary_tablespace:
            sql += 'temporary tablespace %s ' % temporary_tablespace
        if profile:
            sql += 'profile %s ' % profile
        if locked:
            sql += 'account lock '
        if expired:
            sql += 'password expire '

        execute_ddl(sql)

        module.exit_json(msg='User %s has been created.' % schema_name, changed=True, diff=diff, ddls=ddls)


def ensure_absent(schema_name):
    """Drop the user if it exists"""
    prev_user = get_existing_user(schema_name)

    if prev_user and prev_user['oracle_maintained']:
        module.fail_json(msg='Cannot drop a system user.', changed=False)
    elif prev_user:
        execute_ddl('drop user %s cascade' % schema_name)
        module.exit_json(msg='User %s dropped.' % schema_name, changed=True, diff=diff, ddls=ddls)
    else:
        module.exit_json(msg="User %s doesn't exist." % schema_name, changed=False, diff=diff, ddls=ddls)


def main():
    global module
    global cursor
    global diff
    global ddls
    global dsn

    module = AnsibleModule(
        argument_spec=dict(
            authentication_type=dict(type='str', required=False,
                                     choices=['external', 'global', 'no_authentication', 'password']),
            default_tablespace=dict(type='str', default=None),
            expired=dict(type='bool', default=None),
            hostname=dict(type='str', default='localhost'),
            locked=dict(type='bool', default=None),
            mode=dict(type='str', default='normal', choices=['normal', 'sysdba']),
            oracle_home=dict(type='str', required=False),
            password=dict(type='str', required=False, no_log=True),
            port=dict(type='int', default=1521),
            profile=dict(type='str', default=None),
            schema_name=dict(type='str', required=True, aliases=['name']),
            schema_password=dict(type='str', default=None, no_log=True),
            service_name=dict(type='str', required=True, aliases=['tns']),
            state=dict(type='str', default='present', choices=['absent', 'empty', 'present']),
            temporary_tablespace=dict(type='str', default=None),
            username=dict(type='str', required=False, aliases=['user']),
        ),
        supports_check_mode=True,
    )

    authentication_type = module.params['authentication_type']
    default_tablespace = module.params['default_tablespace']
    expired = module.params['expired']
    hostname = module.params['hostname']
    locked = module.params['locked']
    mode = module.params['mode']
    oracle_home = module.params['oracle_home']
    password = module.params['password']
    port = module.params['port']
    profile = module.params['profile']
    schema_name = module.params['schema_name']
    schema_password = module.params['schema_password']
    service_name = module.params['service_name']
    state = module.params['state']
    temporary_tablespace = module.params['temporary_tablespace']
    username = module.params['username']

    # Transforming parameters
    if schema_password:
        authentication_type = 'password'

    if oracle_home:
        os.environ['ORACLE_HOME'] = oracle_home

    # Setting connection
    connection_parameters = {}
    if username and password:
        connection_parameters['user'] = username
        connection_parameters['password'] = password
        connection_parameters['dsn'] = cx_Oracle.makedsn(host=hostname, port=port, service_name=service_name)
    else:  # Using Oracle wallet
        connection_parameters['dsn'] = service_name

    if mode == 'sysdba':
        connection_parameters['mode'] = cx_Oracle.SYSDBA

    # Connecting
    try:
        connection = cx_Oracle.connect(**connection_parameters)
        cursor = connection.cursor()
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code)

    dsn = connection_parameters['dsn']

    diff = {'before': {'schema_name': schema_name},
            'after': {'state': state,
                      'schema_name': schema_name, }}

    ddls = []

    if state in ['empty', 'present']:
        ensure_present(schema_name, authentication_type, schema_password, default_tablespace, temporary_tablespace,
                       profile, locked, expired, state == 'empty')
    elif state == 'absent':
        ensure_absent(schema_name)


if __name__ == '__main__':
    main()
