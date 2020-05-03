#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2014 Mikael Sandström <oravirt@gmail.com>
# Copyright: (c) 2020, Ari Stark <ari.stark@netcourrier.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

import cx_Oracle
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.basic import os

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'}

DOCUMENTATION = '''
module: oracle_role
short_description: Manage Oracle role objects.
description:
    - This module manage Oracle role objects.
    - It handles creation and deletion of roles.
    - It doesn't support changing password. There's no hint to know a password was changed, so no change is made.
version_added: "1.9.1"
author:
    - Mikael Sandström (@oravirt)
    - Ari Stark (@ari-stark)
options:
    identified_method:
        description:
            - Specify the authentication method to use to connect with role.
        default: none
        type: str
        choices: ['none', 'password', 'application', 'external', 'global']
    identified_value:
        description:
            - This is the value to use using authentication by password or using a package.
            - Required if I(identified_method=password) or I(identified_method=application). 
        type: str
    hostname:
        description:
            - Specify the host name or IP address of the database server computer.
        default: localhost
        type: str
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
    role:
        description:
            - The name of the role to create/alter/drop.
            - The name is changed in upper case.
        required: true
        type: str
    service_name:
        description:
            - Specify the service name of the database you want to access.
        required: true
        type: str
    state:
        description:
            - Specify the state of the role.
        default: present
        type: str
        choices: ['present', 'absent']
    username:
        description:
            - Set the login to use to connect the database server.
            - Must not be set if using Oracle wallet.
        type: str
        aliases: ['user']
requirements:
    - Python module cx_Oracle
    - Oracle basic tools.
notes:
    - Check mode and diff mode are supported.
    - Changes made by @ari-stark broke previous module interface.
'''

EXAMPLES = '''
# Ensure role exists
oracle_role:
    hostname: remote-db-server
    service_name: orcl
    user: system
    password: manager
    role: myrole
    state: present

# Set the password "bar" to a role
oracle_role:
    hostname: remote-db-server
    service_name: orcl
    user: system
    password: manager
    role: myrole
    state: present
    identified_method: password
    identified_value: bar

# Ensure role doesn't exist
oracle_role:
    hostname: localhost
    service_name: orcl
    user: system
    password: manager
    role: myrole
    state: absent
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
global diff
global ddls


def execute_select(sql, params=None):
    """Executes a select query and return fetched data"""
    if params is None:
        params = {}
    try:
        return cursor.execute(sql, params).fetchall()
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code, request=sql, params=params)


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


def get_existing_role(role):
    """Get the existing role with is authentication type"""
    result = execute_select('select role, authentication_type from dba_roles where role = :role',
                            {'role': role})

    if result:
        role = result[0][0]
        authentication_type = result[0][1]
        diff['before']['state'] = 'present'
        diff['before']['identified_method'] = authentication_type
        return {'role': role, 'authentifcation_type': authentication_type}
    else:
        diff['before']['state'] = 'absent'
        return None


def ensure_present(role, identified_method, identified_value):
    """Create or alter role if needed. This function doesn't change password if it is already set."""
    prev_role = get_existing_role(role)

    # If role is already defined and there's no change to make.
    if prev_role and prev_role['authentifcation_type'] == identified_method:
        module.exit_json(msg='The role (%s) already exists' % role, changed=False)

    if not prev_role:
        sql = 'create role %s ' % role
    else:
        sql = 'alter role %s ' % role

    if identified_method == 'PASSWORD':
        sql += 'identified by "%s"' % identified_value
    elif identified_method == 'APPLICATION':
        sql += 'identified using %s' % identified_value
    elif identified_method == 'EXTERNAL':
        sql += 'identified externally'
    elif identified_method == 'GLOBAL':
        sql += 'identified globally'
    else:
        sql += 'not identified'

    execute_ddl(sql)
    if not prev_role:
        module.exit_json(msg="Role '%s' created." % role, changed=True, diff=diff, ddls=ddls)
    else:
        module.exit_json(msg="Role '%s' changed." % role, changed=True, diff=diff, ddls=ddls)


def ensure_absent(role):
    """Drop the role if needed."""
    if get_existing_role(role):
        execute_ddl('drop role %s' % role)
        module.exit_json(msg="Role '%s' dropped." % role, changed=True, diff=diff, ddls=ddls)
    else:
        module.exit_json(msg="Role '%s' already absent." % role, changed=False)


def main():
    global module
    global cursor
    global diff
    global ddls

    module = AnsibleModule(
        argument_spec=dict(
            identified_method=dict(type='str', default='none',
                                   choices=['none', 'password', 'application', 'external', 'global']),
            identified_value=dict(type='str', default=None, no_log=True),
            hostname=dict(type='str', default='localhost'),
            mode=dict(type='str', default='normal', choices=['normal', 'sysdba']),
            oracle_home=dict(type='str', required=False),
            password=dict(type='str', no_log=True),
            port=dict(type='int', default=1521),
            role=dict(type='str', required=True),
            service_name=dict(type='str', required=True),
            state=dict(type='str', default='present', choices=['present', 'absent']),
            username=dict(type='str', aliases=['user']),
        ),
        required_together=[['username', 'password']],
        required_if=[['identified_method', 'password', ['identified_value']],
                     ['identified_method', 'application', ['identified_value']]],
        supports_check_mode=True,
    )

    identified_method = module.params['identified_method']
    identified_value = module.params['identified_value']
    hostname = module.params['hostname']
    mode = module.params['mode']
    oracle_home = module.params['oracle_home']
    password = module.params['password']
    port = module.params['port']
    role = module.params['role']
    service_name = module.params['service_name']
    state = module.params['state']
    username = module.params['username']

    # Transforming parameter
    role = role.upper()
    identified_method = identified_method.upper()

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

    ddls = []
    diff = {'before': {'role': role},
            'after': {'role': role, 'state': state, 'identified_method': identified_method}}

    if state == 'present':
        ensure_present(role, identified_method, identified_value)
    elif state == 'absent':
        ensure_absent(role)


if __name__ == '__main__':
    main()
