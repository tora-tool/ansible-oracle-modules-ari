#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2017 Ilmar Kerm <ilmar.kerm@gmail.com>
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
---
module: oracle_facts
short_description: Returns some facts about Oracle DB
description:
    - This module returns some facts about Oracle database.
    - It has several subsets and will gather all subsets by default.
version_added: "2.2.1"
author:
    - Ilmar Kerm (@ilmarkerm)
    - Ari Stark (@ari-stark)
options:
    gather_subset:
        description:
            - Specify the subset to gather.
            - 'min' and 'database' are aliases and will get the same facts.
            - 'all' will gather all possible facts.
            - Every other choice will lead to get 'min' facts and others asked for.
        default: all
        type: list
        choices: ['all', 'database', 'instance', 'min', 'option', 'parameter', 'pdb', 'rac', 'redolog', 'tablespace',
                  'userenv', 'user']
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
    service_name:
        description:
            - Specify the service name of the database you want to access.
        required: true
        type: str
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
    - Check mode is supported and won't change anything as this module don't make anything.
    - Diff mode is not supported, as this module don't make anything.
    - Oracle RDBMS 10gR2 or later required.
'''

EXAMPLES = '''
- name: gather all database facts
  oracle_facts:
    hostname: "192.168.56.101"
    port: 1521
    service_name: "orcl"
    username: "system"
    password: "oracle"
  register: dbfacts

- name: gather 'min' and 'parameter' facts
  oracle_facts:
    hostname: "192.168.56.101"
    port: 1521
    service_name: "orcl"
    username: "system"
    password: "oracle"
    gather_subset: "parameter"
  register: dbfacts

- name: gather 'min', 'parameter' and 'tablespace' facts
  oracle_facts:
    hostname: "192.168.56.101"
    port: 1521
    service_name: "orcl"
    username: "system"
    password: "oracle"
    gather_subset:
        - "parameter"
        - "tablespace"
  register: dbfacts
'''

RETURN = '''
version:
    description: Contains the database version
    type: str
database:
    description: Contains content of v$database.
    type: dict
    elements: str
instance:
    description: Contains content of v$instance.
    returned: if I(instance) or I(all) is in requested subset.
    type: dict
    elements: str
options:
    description: Contains content of v$option.
    returned: if I(option) or I(all) is in requested subset.
    type: dict
    elements: str
parameters:
    description: Contains content of v$parameter.
    returned: if I(parameter) or I(all) is in requested subset.
    type: dict
    elements: dict
pdbs:
    description: Contains content of v$pdb.
    returned: if I(pdb) or I(all) is in requested subset.
    type: list
    elements: dict
racs:
    description: Contains content of gv$instance.
    returned: if I(rac) or I(all) is in requested subset.
    type: list
    elements: dict
redologs:
    description: Contains content of v$log.
    returned: if I(redolog) or I(all) is in requested subset.
    type: list
    elements: dict
tablespaces:
    description: Contains content of v$tablespace and v$datafile.
    returned: if I(tablespace) or I(all) is in requested subset.
    type: list
    elements: dict
temp_tablespaces:
    description: Contains content of v$tablespace and v$tempfile.
    returned: if I(tablespace) or I(all) is in requested subset.
    type: list
    elements: dict
userenv:
    description: Contains some data of current user.
    returned: if I(userenv) or I(all) is in requested subset.
    type: dict
    elements: str
users:
    description: Contains content of all_users.
    returned: if I(user) or I(all) is in requested subset.
    type: list
    elements: dict
'''

global module
global cursor


def execute_select(sql):
    """Executes a select query and return fetched data"""
    try:
        return cursor.execute(sql).fetchall()
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code, request=sql)


def execute_select_to_dict(sql):
    """Executes a select query and return a list of dictionaries : one dictionary for each row"""
    try:
        cursor.execute(sql)
        column_names = [description[0].lower() for description in
                        cursor.description]  # First element is the column name.
        return [dict(zip(column_names, row)) for row in cursor]
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code, request=sql)


def get_database():
    """Get the v$database content"""
    return execute_select_to_dict('select * from v$database')[0]  # The table is a oneliner.


def get_instance():
    """Get the v$instance content"""
    return execute_select_to_dict('select * from v$instance')[0]  # The table is a oneliner.


def get_options():
    """Get the v$option content"""
    options = execute_select('select parameter, value from v$option order by parameter')
    parameters, values = zip(*options)
    return dict(zip(parameters, values))


def get_parameters():
    """Get the v$parameter content"""
    param_list = execute_select('select name, value, isdefault from v$parameter order by name')
    names, values, isdefaults = zip(*param_list)  # Splits...
    return {names[i]: {'value': values[i], 'isdefault': isdefaults[i]} for i in range(0, len(names))}  # ... and groups.


def get_pdbs():
    """Get the v$pdbs content"""
    return execute_select_to_dict(
        'select con_id, rawtohex(guid) guid_hex, name, open_mode, total_size from v$pdbs order by name')


def get_racs():
    """Get the gv$instance content"""
    return execute_select_to_dict(
        'select inst_id, instance_name, host_name, startup_time from gv$instance order by inst_id')


def get_redologs():
    """Get the v$log content"""
    return execute_select_to_dict(
        'select group#, thread#, sequence#, round(bytes/power(1024, 2)) mb, blocksize, archived, status'
        '  from v$log'
        ' order by thread#, group#')


def get_tablespaces():
    """Get the v$tablespace and v$datafile content"""
    return execute_select_to_dict(
        'select ts.con_id, ts.name, ts.bigfile, df.name datafile_name, round(df.bytes/power(1024, 2)) size_mb'
        '  from v$tablespace ts, v$datafile df'
        ' where df.con_id = ts.con_id and df.ts# = ts.ts#'
        ' order by ts.con_id, ts.name, df.name')


def get_temp_tablespaces():
    """Get the v$tablespace and v$tempfile content"""
    return execute_select_to_dict(
        'select ts.con_id, ts.name, ts.bigfile, tf.name tempfile_name, round(tf.bytes/power(1024, 2)) size_mb'
        '  from v$tablespace ts, v$tempfile tf'
        ' where tf.con_id = ts.con_id and tf.ts# = ts.ts#'
        ' order by ts.con_id, ts.name, tf.name')


def get_userenv():
    """Get data of current user"""
    return execute_select_to_dict("select sys_context('USERENV','CURRENT_USER') current_user,"
                                  "       sys_context('USERENV','DATABASE_ROLE') database_role,"
                                  "       sys_context('USERENV','ISDBA') isdba,"
                                  "       sys_context('USERENV','ORACLE_HOME') oracle_home,"
                                  "       to_number(sys_context('USERENV','CON_ID')) con_id,"
                                  "       sys_context('USERENV','CON_NAME') con_name"
                                  "  from dual")[0]


def get_users():
    """Get the all_users content"""
    return execute_select_to_dict(
        "select username, user_id, created from all_users where oracle_maintained = 'N' order by username")


# Ansible code
def main():
    global module
    global cursor

    module = AnsibleModule(
        argument_spec=dict(
            gather_subset=dict(type='list',
                               default=['all'],
                               choices=['all', 'database', 'instance', 'min', 'option', 'parameter', 'pdb', 'rac',
                                        'redolog', 'tablespace', 'userenv', 'user']),
            hostname=dict(type='str', default='localhost'),
            mode=dict(type='str', default='normal', choices=['normal', 'sysdba']),
            oracle_home=dict(type='str', required=False),
            password=dict(type='str', required=False, no_log=True),
            port=dict(type='int', default=1521),
            service_name=dict(type='str', required=True),
            username=dict(type='str', required=False, aliases=['user']),
        ),
        required_together=[['username', 'password']],
        supports_check_mode=True,
    )

    # Connect to database
    gather_subset = set(module.params['gather_subset'])
    hostname = module.params['hostname']
    mode = module.params['mode']
    oracle_home = module.params['oracle_home']
    password = module.params['password']
    port = module.params['port']
    service_name = module.params['service_name']
    username = module.params['username']

    if 'all' in gather_subset:
        gather_subset.remove('all')
        gather_subset.update(
            ['database', 'instance', 'option', 'parameter', 'pdb', 'rac', 'redolog', 'tablespace', 'userenv', 'user'])
    if 'min' in gather_subset:
        gather_subset.remove('min')
        gather_subset.add('database')
    if oracle_home:
        os.environ['ORACLE_HOME'] = oracle_home

    version = None

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
        version = connection.version
        cursor = connection.cursor()
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        module.fail_json(msg=error.message, code=error.code)

    if version < '12.0':
        module.fail_json(msg='Database version must be 12 or greater.', changed=False)

    facts = {'version': version}
    # Execute PL/SQL to return some additional facts

    # database/min subset is always done.
    database = get_database()
    facts['database'] = database

    if 'instance' in gather_subset:
        facts['instance'] = get_instance()

    if 'option' in gather_subset:
        facts['options'] = get_options()

    if 'parameter' in gather_subset:
        facts['parameters'] = get_parameters()

    if 'pdb' in gather_subset:
        facts['pdbs'] = get_pdbs()

    if 'rac' in gather_subset:
        facts['racs'] = get_racs()

    if 'redolog' in gather_subset:
        facts['redologs'] = get_redologs()

    if 'tablespace' in gather_subset:
        facts['tablespaces'] = get_tablespaces()
        facts['temp_tablespaces'] = get_temp_tablespaces()

    if 'userenv' in gather_subset:
        facts['userenv'] = get_userenv()

    if 'user' in gather_subset:
        facts['users'] = get_users()

    module.exit_json(changed=False, oracle_facts=facts)


if __name__ == '__main__':
    main()
