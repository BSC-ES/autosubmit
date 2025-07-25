# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import os
import sqlite3
import textwrap
import traceback
from typing import Dict, List

from autosubmit.log.log import Log


def get_structure(exp_id, structures_path):
    # type: (str, str) -> Dict[str, List[str]]
    """
    Creates file of database and table of experiment structure if it does not exist. Returns current structure.

    :return: Map from experiment name source to name destination  
    :rtype: Dictionary Key: String, Value: List(of String)
    """
    try:        
        if os.path.exists(structures_path):
            db_structure_path = os.path.join(
                structures_path, "structure_" + exp_id + ".db")
            if not os.path.exists(db_structure_path):
                open(db_structure_path, "w")
            # print(db_structure_path)
            conn = create_connection(db_structure_path)
            create_table_query = textwrap.dedent(
                '''CREATE TABLE
            IF NOT EXISTS experiment_structure (
            e_from text NOT NULL,
            e_to text NOT NULL,
            UNIQUE(e_from,e_to)
            );''')
            create_table(conn, create_table_query)
            current_table = _get_exp_structure(db_structure_path)            
            current_table_structure = dict()
            for item in current_table:
                _from, _to = item
                current_table_structure.setdefault(_from, []).append(_to)
                current_table_structure.setdefault(_to, [])
            return current_table_structure            
        else:            
            raise Exception("Structures folder not found " +
                            str(structures_path))
    except Exception as exp:
        Log.printlog("Get structure error: {0}".format(str(exp)), 6014)
        Log.debug(traceback.format_exc())
        


def create_connection(db_file):
    """ 
    Create a database connection to the SQLite database specified by db_file.  
    :param db_file: database file name  
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        return None


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        Log.printlog("Create table error {0}".format(str(e)), 5000)


def _get_exp_structure(path):
    """
    Get all registers from experiment_status.\n
    :return: row content: exp_id, name, status, seconds_diff  
    :rtype: 4-tuple (int, str, str, int)
    """
    try:
        conn = create_connection(path)
        conn.text_factory = str
        cur = conn.cursor()
        cur.execute(
            "SELECT e_from, e_to FROM experiment_structure")
        rows = cur.fetchall()
        return rows
    except Exception as exp:
        Log.debug(
            "Get structure error {0}, couldn't load from storage ".format(str(exp)))
        Log.debug(traceback.format_exc())
        return dict()


def save_structure(graph, exp_id, structures_path):
    """
    Saves structure if path is valid
    """
    conn = None
    # Path to structures folder exists
    if os.path.exists(structures_path):
        # Path to structure file
        db_structure_path = os.path.join(
            structures_path, "structure_" + exp_id + ".db")
        # Path to structure file exists -> delete
        if os.path.exists(db_structure_path):
            conn = create_connection(db_structure_path)
            _delete_table_content(conn)
        else:
            # Path to structure file does not exist -> Initialize structure
            get_structure(exp_id, structures_path)
            conn = create_connection(db_structure_path)
        if conn:
            # Save structure
            nodes_edges = {u for u, v in graph.edges()}
            nodes_edges.update({v for u, v in graph.edges()})
            independent_nodes = {
                u for u in graph.nodes() if u not in nodes_edges}
            data = {(u, v) for u, v in graph.edges()}
            data.update({(u, u) for u in independent_nodes})
            # save
            _create_edge(conn, data)
            conn.commit()
    else:
        # Structure Folder not found
        raise Exception(
            "Structures folder not found {0}".format(structures_path))


def _create_edge(conn, data):
    """
    Create edge
    """
    try:
        sql = ''' INSERT INTO experiment_structure(e_from, e_to) VALUES(?,?) '''
        cur = conn.cursor()
        cur.executemany(sql, data)
        # return cur.lastmod
    except sqlite3.Error as e:
        Log.debug(traceback.format_exc())
        Log.warning("Error on Insert : {0}".format(str(type(e).__name__)))


def _delete_table_content(conn):
    """
    Deletes table content
    """
    try:
        sql = ''' DELETE FROM experiment_structure '''
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        Log.debug(traceback.format_exc())
        Log.warning("Error on Delete : {0}".format(str(type(e).__name__)))
