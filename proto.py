import gc
import sys
import logging
import re
import time
from datetime import datetime, timezone
from pprint import pprint

from collections import deque

from anytree import Node, AnyNode, SymlinkNode, RenderTree, search, cachedsearch
from anytree.resolver import Resolver, ChildResolverError
from anytree import util
from anytree.iterators.preorderiter import PreOrderIter

import json
from jedi.inference.value import instance
#import jmespath
from path_dict import PathDict
from copy import deepcopy
from dictdiffer import diff, patch, swap, revert

from contextlib import suppress


class Auth(object):

    def __init__(self, session_id, nst_token):
        self.session_id = session_id
        self.nst_token = nst_token

class Message(object):

    def __init__(self, type, topic, body):
        self.type = type
        self.topic = topic
        self.body = body

    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__.__name__, self.type, self.topic, self.body)

    def __str__(self):
        return self.__repr__()

class M100(object):
    def __init__(self, data):
        self.data = data

class Command(object):
    def __init__(self, name, values):
        self.name = name
        self.values = values

class ServerTime(object):
    def __init__(self, time):
        self.time = time

class Config(object):
    def __init__(self, data):
        self.data = data

class Subscription(object):

    def __init__(self, topics: list):
        self.topics = topics

class InsertData(object):
    def __init__(self, id, path, data):
        self.id = id
        self.path = path
        self.data = data

class UpdateData(object):
    def __init__(self, id, path, data):
        self.id = id
        self.path = path
        self.data = data

class RemoveData(object):
    def __init__(self, id, path):
        self.id = id
        self.path = path

class OvInPlay(object):
    def __init__(self):
        pass

class Protocol():

    _DELIMITERS_RECORD = u'\x01'
    _DELIMITERS_FIELD = u'\x02'
    _DELIMITERS_HANDSHAKE = u'\x03'
    _DELIMITERS_MESSAGE = u'\x08'

    _ENCODINGS_NONE = u'\x00'

    _TYPES_TOPIC_LOAD_MESSAGE = u'\x14'
    _TYPES_DELTA_MESSAGE = u'\x15'
    _TYPES_SUBSCRIBE = u'\x16'
    _TYPES_PING_CLIENT = u'\x19'
    _TYPES_TOPIC_STATUS_NOTIFICATION = u'\x23'

    _MESSAGES_SESSION_ID = u'%s%sP%s__time,S_%%s%s' % (
        _TYPES_TOPIC_STATUS_NOTIFICATION,
        _DELIMITERS_HANDSHAKE,
        _DELIMITERS_RECORD,
        _ENCODINGS_NONE,
    )

    _MESSAGES_COMMAND_NST = u'%s%scommand%snst%s%%s%sSPTBK' % (
        _DELIMITERS_FIELD,
        _ENCODINGS_NONE,
        _DELIMITERS_RECORD,
        _DELIMITERS_RECORD,
        _DELIMITERS_FIELD,
    )

    _MESSAGES_SUBSCRIPTION = u'%s%s%%s%s' % (
        _TYPES_SUBSCRIBE,
        _ENCODINGS_NONE,
        _DELIMITERS_RECORD,
    )

    _OPCODE_INIT = 'F'
    _OPCODE_UPDATE = 'U'
    _OPCODE_INSERT = 'I'
    _OPCODE_DELETE = 'D'

    _TOPICS_DELIM = ','
    _RECORD_DELIM = '|'
    _FIELD_DELIM   = ';'
    _KEYVALUE_DELIM = '='

    _PATH_DELIM = '/'

    _AUTH_TOKEN_DELIM = ','


    _SCORE_DELIM = '-'
    _TEAMS_DELIM = ' v '

    CL_SOCCER_ID = 1
    CL_AMERICAN_FOOTBALL_ID = 12
    CL_BANDY_ID = 89
    CL_BASEBALL_ID = 16
    CL_BASKETBALL_ID = 18
    CL_BEACH_VOLLYEBALL_ID = 95
    CL_BOWLS_ID = 66
    CL_BOXING_MMA_ID = 9
    CL_CRICKET_ID = 3
    CL_CYCLING_ID = 38
    CL_DARTS_ID = 15
    CL_ESPORTS_ID = 151
    CL_FUTSAL_ID = 83
    CL_GAELIC_SPORTS_ID = 75
    CL_GOLF_ID = 7
    CL_GREYHOUNDS_ID = 4
    CL_HORSE_RACING_ID = 2
    CL_ICE_HOCKEY_ID = 17
    CL_LOTTO_ID = 6
    CL_MOTOR_FORMULA_1_ID = 10
    CL_MOTOR_NASCAR_ID = 65
    CL_MOTOR_RALLY_ID = 116
    CL_POLITICS_ID = 157
    CL_RUGBY_LEAGUE_ID = 19
    CL_RUGBY_UNION_ID = 8
    CL_SNOOKER_ID = 14
    CL_SPECIALS_DENMARK_ID = 133
    CL_SPECIALS_UNITED_KINGDOMS_ID = 5
    CL_SQUASH_ID = 107
    CL_SPEEDWAY_ID = 24
    CL_TABLE_TENNIS_ID = 92
    CL_TENNIS_ID = 13
    CL_TROTTING_ID = 88
    CL_VIRTUAL_SPORTS_ID = 144
    CL_VOLLEYBALL_ID = 91
    CL_FLOORBALL_ID = 90
    CL_HANDBALL_ID = 78
    CL_WATER_POLO_ID = 110
    CL_TEN_PIN_BOWLING_ID = 128


    TYPE_CLASSIFICATION = 'CL'
    TYPE_COMPETITION = 'CT'
    TYPE_EVENT = 'EV'
    TYPE_MARKET_GROUP = 'MG'
    TYPE_MARKET = 'MA'
    TYPE_COLUMN = 'CO'
    TYPE_PARTICIPANT = 'PA'
    TYPE_TEAM_GROUP = 'TG'
    TYPE_TEAM = 'TE'
    TYPE_STAT_GROUP = 'SG'
    TYPE_STAT = 'ST'
    TYPE_EXTRA_SCORES = 'ES'
    TYPE_SCORE_COLUMN = 'SC'
    TYPE_SCORE_CELL = 'SL'

    _TYPE_HIERARCHY = {
        TYPE_CLASSIFICATION: [None],
        TYPE_COMPETITION: [TYPE_CLASSIFICATION],
        TYPE_EVENT: [TYPE_COMPETITION, TYPE_CLASSIFICATION],
        TYPE_MARKET_GROUP: [TYPE_EVENT],
        TYPE_MARKET: [TYPE_MARKET_GROUP, TYPE_EVENT],
        TYPE_COLUMN: [TYPE_MARKET],
        TYPE_PARTICIPANT: [TYPE_COLUMN, TYPE_MARKET],
        TYPE_TEAM_GROUP: [TYPE_EVENT],
        TYPE_TEAM: [TYPE_TEAM_GROUP],
        TYPE_STAT_GROUP: [TYPE_EVENT],
        TYPE_STAT: [TYPE_STAT_GROUP],
        TYPE_EXTRA_SCORES: [TYPE_EVENT],
        TYPE_SCORE_COLUMN: [TYPE_EXTRA_SCORES],
        TYPE_SCORE_CELL: [TYPE_SCORE_COLUMN]
    }

    RE_CLASSIFICATION = r'OV_(\d+)'
    RE_COMPETITION = r'OV([ -~]+)C(\d+)G?_\d+_\d+'
    RE_EVENT = r'(?:(?:O|6)V)?(\d+)(?:C|M)(\d+)A?'
    RE_MARKET = r'OV(\d+)C(\d+)(?:-(\d+))?'
    RE_PARTITIPANT = r'OV?(\d+)(?:-(\d+))?'
    RE_COLUMN = r'OV(\d+)C(\d+)-(\d+)-(\d+)'

    RE_MEDIA = r'(\d+)M\d+_\d+'

    dictfilt = lambda self, x, y: dict([(i, x[i]) for i in x if i in set(y)])
    intersectlist = lambda self, x, y: list(set(x) & set(y))

    def __init__(self, on_insert=None, on_update=None, on_remove=None, on_classification_found=None, classifications={}):

        self.table = {}
        self.tree_data = AnyNode(IT='/')
        self.tree_resolver = Resolver('IT')

        self.mapping_data = PathDict({})
        self.mapping_table = PathDict({})
        self.mapping_cache = deque([])

        self.classifications = classifications

        self.on_insert = on_insert
        self.on_update = on_update
        self.on_remove = on_remove
        self.on_classification_found = on_classification_found

    @staticmethod
    def parseServerTime(type, params):
        time_str = params['TI']
        return time.strptime(time_str, '%Y%m%d%H%M%S%f')

    @classmethod
    def msgSessionId(self, session_id, nst_token):
        return self._MESSAGES_SESSION_ID % self._AUTH_TOKEN_DELIM.join([session_id, nst_token])

    @classmethod
    def msgSubscription(self, topics):
        return self._MESSAGES_SUBSCRIPTION % self._TOPICS_DELIM.join(topics)

    def parseParamsData(self, type, data):

        def parseTimer(tu):
            try:
                return int(datetime.strptime(tu, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                return None

        def parse_score(data):
            try:
                return tuple(map(list, zip(*map(lambda x: map(int, x.split(self._SCORE_DELIM)), data.split(',')))))
            except Exception:
                return (None, None)

        def parse_odd(data):
            try:
                (a, b) = data.split('/')
                return float('%0.3f' % float((int(a) / int(b)) + 1))
            except:
                return None

        def parse_hd(data):
            parts = str(data).strip().split(',')
            try:
                if len(parts) == 2:
                    return str((float(parts[0])+float(parts[1]))/2)
                else:
                    return str(data).strip()
            except:
                return None

        result = {}
        params = []
        if type == self.TYPE_CLASSIFICATION:

            params = [
                ('NA', 'id', lambda x: str(x).lower().replace(' ', '_'), None),
                ('NA', 'name', None, None),
            ]

            if not int(data['ID']) in self.classifications.keys():
                self.classifications.update(
                    self.on_classification_found(data['ID'], data['NA'])
                )

        elif type == self.TYPE_COMPETITION:

            params = [
                ('IT', ('id', 'cid'), lambda x: re.search(self.RE_COMPETITION, x).groups(), {'cid': lambda x: int(x)}),
                ('NA', 'name', None, None),
            ]

        elif type == self.TYPE_EVENT:

            params = [
                ('IT', ('id', 'cid'), lambda x: re.search(self.RE_EVENT, x).groups(), {'cid': lambda x: int(x)}),
                ('NA', 'name', None, None),
                ('CT', 'league_name', None, None),
                ('SS', 'score', None, None),
                ('TM', 'time_minutes', lambda x: int(x), None),
                ('TS', 'time_seconds', lambda x: int(x), None),
                ('TT', 'time_ticking', lambda x: bool(int(x)), None),
                ('TU', 'time_stamp', lambda x: parseTimer(x), None),
                ('CP', 'current_period', None, None),
                ('MP', 'match_postponed', None, None),
                ('MD', 'matchlive_period', None, None),
                ('TD', 'countdown', None, None),
                ('MS', 'media_id', None, None),
                ('VI', 'video_avaliable', lambda x: bool(int(x)), None),
                ('SM', 'start_time', None, None),
                ('XP', 'short_points', None, None),
                ('C1', 'c1', None, None),
                ('T1', 't1', None, None),
                ('C2', 'c2', None, None),
                ('T2', 't2', None, None),
                ('FI', 'fi', None, None),
                ('S1', 's1', None, None),
                ('S2', 's2', None, None),
                ('S3', 's3', None, None),
                ('S4', 's4', None, None),
                ('S5', 's5', None, None),
                ('S6', 's6', None, None),
                ('S7', 's7', None, None),
                ('S8', 's8', None, None),
                ('S9', 's9', None, None),
                ('OR', 'order', lambda x: int(x), None),
                ('ID', 'sid', lambda x: re.search(self.RE_EVENT, x).group(1) if x else None, None),
            ]


        elif type == self.TYPE_MARKET_GROUP:

            params = [
                ('ID', 'id', None, None),
                ('IT', 'it', None, None),
                ('NA', 'name', None, {'name': None}),
                ('OR', 'order', lambda x: int(x), None),
                ('SU', 'suspended', lambda x: bool(int(x)), {'suspended': False}),
                ('MT', 'participant_count', None, None),
                ('DO', 'default_open', None, {'default_open': lambda x: bool(int(x) if x else 0)}),
                ('SY', 'visibility', None, {'visibility': lambda x: True if x == 'ig' else False}),
            ]

        elif type == self.TYPE_MARKET:

            params = [
                ('ID', 'id', None, None),
                ('FI', 'fi', lambda x: int(x), None),
                ('NA', 'name', None, None),
                ('OR', 'order', lambda x: int(x), {'order': None}),
                ('MT', 'participant_count', None, None),
                ('DX', 'disable_column_distribution', None, {'disable_column_distribution': lambda x: bool(int(x) if x else 0)}),
                ('SY', 'sy', None, None),
                ('PY', 'py', None, None),
            ]

        elif type == self.TYPE_COLUMN:

            params = [
                ('ID', 'id', None, None),
                ('NA', 'name', None, None),
                ('CN', 'column_number', None, None),
                ('OR', 'order', lambda x: int(x), None),
                ('SY', 'style', None, None),
            ]

        elif type == self.TYPE_PARTICIPANT:

            params = [
                ('ID', 'id', None, None),
                ('NA', 'name', None, None),
                ('OR', 'order', lambda x: int(x), {'order': None}),
                ('HA', 'ha', None, None),
                ('HD', 'hd', lambda x: parse_hd(x.strip()), None),
                ('OD', 'odd', lambda x: parse_odd(x.strip()), None),
                ('TN', 'trainer_name', None, None),
                ('JY', 'jockey', None, None),
                ('IG', 'image_id', None, None),
                #('FI', 'fi', None, None),
                ('SU', 'suspended', lambda x: bool(int(x)), {'suspended': False}),
            ]

        elif type == self.TYPE_TEAM_GROUP:

            params = [
                ('ID', 'id', None, None),
                ('CT', 'sport_name', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        elif type == self.TYPE_TEAM:

            params = [
                ('ID', 'id', lambda x: int(x), None),
                ('NA', 'name', None, None),
                ('PI', 'playing_indicator', None, None),
                ('PO', 'points', None, None),
                ('S1', 's1', None, None),
                ('S2', 's2', None, None),
                ('S3', 's3', None, None),
                ('S4', 's4', None, None),
                ('S5', 's5', None, None),
                ('S6', 's6', None, None),
                ('S7', 's7', None, None),
                ('S8', 's8', None, None),
                ('S9', 's9', None, None),
                ('SC', 'score', None, None),
                ('TD', 'countdown', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        elif type == self.TYPE_STAT_GROUP:

            params = [
                ('ID', 'id', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        elif type == self.TYPE_STAT:

            params = [
                ('ID', 'id', None, None),
                ('LA', 'label', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        elif type == self.TYPE_EXTRA_SCORES:

            params = [
                ('ID', 'id', None, None),
                ('NA', 'name', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        elif type == self.TYPE_SCORE_COLUMN:

            params = [
                ('ID', 'id', None, None),
                ('NA', 'name', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        elif type == self.TYPE_SCORE_CELL:

            params = [
                ('ID', 'id', lambda x: int(x), None),
                ('D1', 'd1', None, None),
                ('D2', 'd2', None, None),
                ('OR', 'order', lambda x: int(x), None),
            ]

        else:
            return {}

            (pseudo, name, func, default) = param
            if pseudo in data and data[pseudo] is not None:
                value = func(data[pseudo]) if callable(func) else data[pseudo]
                if isinstance(name, tuple):
                    result.update(dict(zip(name, value)))
                else:
                    result[name] = value

            if isinstance(default, dict):
                for k, v in default.items():
                    if item:= None if not k in result else v(result[k]) if callable(v) else result[k]:
                        result[k] = item

        return result


    @classmethod
    def __find_parent_by_type(self, node, type):
        for parent in node.iter_path_reverse():
            if hasattr(parent, 'type') and parent.type == type:
                return parent
        return None

    @classmethod
    def __find_event(self, node):
        def find_symlink(node, type):
            for v in node.children:
                if isinstance(v, SymlinkNode) and v.type == type:
                    return v
            return None

        if (parent:= self.__find_parent_by_type(node, self.TYPE_EVENT)):
            if (parent:= find_symlink(parent, self.TYPE_EVENT)) and \
                (found:= find_symlink(parent.target, self.TYPE_CLASSIFICATION)):
                    return (parent, found)
        return None


    def processMapping(self, opcode, type, node, keys, caching=False):

        def find_sub_event(node, type):
            if (event:= self.__find_parent_by_type(node, self.TYPE_EVENT)) is not None:
                if not (found:= search.find_by_attr(self.tree_mapping, event.id, name='id', maxlevel=3)) is None:
                    return search.find_by_attr(found, type, name='type', maxlevel=2)
            return None

        def set_attr_dict(node, name, value):
            if hasattr(node, name):
                value = {**getattr(node, name), **value}
            setattr(node, name, value)

        path = None
        data = None

        if type == self.TYPE_CLASSIFICATION:
            if opcode == self._OPCODE_INSERT:
                data = [([node.id], {})]

        elif type == self.TYPE_EVENT:
            if node.IT.startswith('OVG'):
                return []
            elif node.IT.startswith('OV'):
                if (parent:= self.__find_parent_by_type(node, self.TYPE_CLASSIFICATION)):

                    path = [parent.id, node.id]
                    if opcode == self._OPCODE_INSERT:
                        SymlinkNode(parent, parent=node)
                        data = [(path, {**self.dictfilt(node.__dict__, list(filter(lambda x: x not in [], keys))), **{ #'id', 'c2', 't2', 'fi'
                                'teams': {},
                                'scores': {},
                                'stats': {},
                                'markets': {},
                                'summary': {}
                            }})]

                    if opcode == self._OPCODE_UPDATE:
                        if (updated:= self.dictfilt(node.__dict__, keys)):
                            data = [(path, updated)]

            elif node.IT.startswith('6V') or 'M' in node.IT:
                node_id = node.c2
                found = search.findall(self.tree_data, lambda n: n.IT.startswith('OV') and not isinstance(n, SymlinkNode) and hasattr(n, 'c2') and n.c2 == node_id, maxlevel=5)
                for item in found:
                    SymlinkNode(item, parent=node)

                # if opcode == self._OPCODE_INSERT:
                #     if not (nodes:= self.__find_event(node)) is None:
                #         path = [nodes[1].target.id, node.fi if hasattr(node, 'fi') else nodes[0].sid]

                #         if (updated:= self.dictfilt(node.__dict__, keys)):
                #             data = [(path, updated)]
                #             opcode = self._OPCODE_UPDATE

        elif type == self.TYPE_TEAM:
            if not (nodes:= self.__find_event(node)) is None:
                data = []
                if node.IT.startswith('ML'):
                    #if opcode == self._OPCODE_INSERT:
                    headers = self.dictfilt(nodes[0].parent.__dict__, ['s{}'.format(n) for n in range(1, 9)])
                        #data+= [([nodes[1].target.id, nodes[0].fi, 'stats', 'headers'], headers)]
                    if opcode in [self._OPCODE_INSERT, self._OPCODE_UPDATE]:
                        data+= [
                                #([nodes[1].target.id, nodes[0].id, 'teams', node.id], node.name if hasattr(node, 'name') else None),
                                ([nodes[1].target.id, nodes[0].id, 'stats', node.name], {headers[k]: v for k, v in self.dictfilt(self.dictfilt(node.__dict__, keys), headers).items()})
                            ]
                # else:
                #     if opcode in [self._OPCODE_INSERT, self._OPCODE_UPDATE]:
                #         data+= [([nodes[1].target.id, nodes[0].id, 'teams', node.id], node.name if hasattr(node, 'name') else None)]

        elif type == self.TYPE_SCORE_CELL:
            if hasattr(node.parent, 'name') and (parent_name:= node.parent.name) is not None and parent_name:
                if not (nodes:= self.__find_event(node)) is None:
                    if ((found:= self.__find_parent_by_type(node, self.TYPE_EXTRA_SCORES)) and hasattr(node, 'id')):
                        team_node = next(PreOrderIter(found.children[0], filter_=lambda n: n.type == self.TYPE_SCORE_CELL and n.id == node.id, maxlevel=2))
                        if opcode in [self._OPCODE_INSERT, self._OPCODE_UPDATE]:
                            if hasattr(node, 'd1'):
                                data = [([nodes[1].target.id, nodes[0].id if found.IT.startswith('ML') else nodes[0].id, 'scores', team_node.d1, parent_name], node.d1)]

        elif type == self.TYPE_MARKET_GROUP:
            if node.IT.startswith('6V'):
                if not (nodes:= self.__find_event(node)) is None:
                    path = [nodes[1].target.id, nodes[0].id, 'markets', node.id if node.id else node.it]
                    if opcode in [self._OPCODE_INSERT, self._OPCODE_UPDATE]:
                        data = [(path, {'name': node.name if hasattr(node, 'name') else None, 'order': node.order, 'visibility': node.visibility, 'odds': odds.data if not (odds:=self.mapping_data.get_path(path+['odds'])) is None else {}})]

        # elif type == self.TYPE_MARKET:
        #     if not (nodes:= self.__find_event(node)) is None:
        #         if (found:= self.__find_parent_by_type(node, self.TYPE_MARKET_GROUP)):
        #             if (not '-H' in node.IT) and ((not hasattr(found, 'sy') or not hasattr(found, 'py'))):
        #                 found.sy = node.sy
        #                 #found.py = node.py
        #                 opcode = self._OPCODE_UPDATE
        #                 data = [(
        #                     [nodes[1].target.id, nodes[0].id, 'markets', found.id if found.id else found.it],
        #                     {'sy': found.sy}#'py': found.py, 
        #                 )]

        elif type == self.TYPE_PARTICIPANT:
            if node.IT.startswith('6V') and not '-H' in node.IT and hasattr(node, 'odd'):
                if not (nodes:= self.__find_event(node)) is None:
                    if (found:= self.__find_parent_by_type(node, self.TYPE_MARKET_GROUP)):

                        path = [nodes[1].target.id, nodes[0].id, 'markets', found.id if found.id else found.it, 'odds']

                        odds = {} if (odds:= self.mapping_data.get_path(path)) is None else odds.data

                        # if util.leftsibling(node.parent) is None and not hasattr(node, 'odd'):
                        #     odd = odds[node.name] if node.name in odds else get_pa_values(node)
                        #     path+= [node.name]

                        node_name = node.name if hasattr(node, 'name') and node.name else node.hd if hasattr(node, 'hd') and node.hd else node.ha if hasattr(node, 'ha') and node.ha else None
                        odd = {'odd': node.odd, 'suspended': node.suspended if hasattr(node, 'suspended') else False, 'order': node.order, 'sy': node.parent.sy, 'py': node.parent.py if hasattr(node.parent, 'py') else None}

                        if hasattr(node.parent, 'name'):
                            if node_name is not None:
                                if not node.parent.name in odds:
                                    odds[node.parent.name] = {'order': node.parent.order}
                                odds[node.parent.name][node_name] = odd
                                path+= [node.parent.name, node_name]
                            else:

                                podds = odds
                                if not util.leftsibling(node.parent) is None:
                                    ni = node.parent.children.index(node)
                                    if ni < len(found.children[0].children):
                                        header_node = found.children[0].children[ni]

                                    # try:
                                    #     header_node = next(PreOrderIter(found.children[0], filter_=lambda n: n.type == self.TYPE_PARTICIPANT, stop=lambda n: n.order >= node.order, maxlevel=2))
                                        if not hasattr(header_node, 'odd'):
                                            if not header_node.name in odds:
                                                odds[header_node.name] = {'order': node.parent.order}
                                            podds = odds[header_node.name]
                                            path+= [header_node.name]
                                    # except StopIteration:
                                    #     pass

                                podds[node.parent.name] = odd
                                path+= [node.parent.name]

                        else:
                            odds[node_name] = odd
                            path+= [node_name]

                        data = [(path, odd)]


        elif type == self.TYPE_STAT:
            if node.IT.startswith('6V') and hasattr(node, 'label'):
                if not (nodes:= self.__find_event(node)) is None:
                    path = [nodes[1].target.id, nodes[0].id, 'summary', node.id]
                    if opcode == self._OPCODE_INSERT:
                        data = [(path, {'label': node.label, 'order': node.order})]
                    elif opcode == self._OPCODE_UPDATE:
                        if (updated:= self.dictfilt(node.__dict__, self.intersectlist(keys, [
                                'label',
                                'order',
                        ]))):
                            data = [(path, updated)]

        result = []

        while data:
            (path, params) = data.pop(0)

            if opcode == self._OPCODE_INSERT:
                self.mapping_table[node.IT] = deepcopy(path)
                self.mapping_data.set_path(deepcopy(path), params)
                result+= [(opcode, path, params, node)]
            elif opcode == self._OPCODE_UPDATE and params:
                path_mapped = self.mapping_table[node.IT]

                if not (found:= self.mapping_data.get_path(deepcopy(path_mapped))):
                    self.mapping_data.set_path(deepcopy(path_mapped), params)
                    found = params
                elif isinstance(params, dict):
                    found.update(params)
                    found = found.dict
                else:
                    found = params

                if path != path_mapped:
                    diff_path = next(diff(path, path_mapped))
                    result+= [
                            (self._OPCODE_DELETE, path_mapped, None, None),
                        ]
                    if diff_path[0] != 'remove':
                        diff_index = diff_path[1][0]
                        self.mapping_table[node.IT] = path
                        self.mapping_data.set_path(deepcopy(path[:diff_index+1]), found)
                        if not (found_drop:= self.mapping_data.get_path(path_mapped[:-1])) is None:
                            found_drop.pop(path_mapped[-1])

                        result+= [
                                (self._OPCODE_INSERT, path[:diff_index+1], found, node),
                            ]
                else:
                    result+= [(opcode, path, params, node)]

            if caching:
                self.mapping_cache.append((path, params, node))

        return result

    def insertMapping(self, type, node, keys, caching = False):
        return self.processMapping(self._OPCODE_INSERT, type, node, keys, caching=caching)

    def updateMapping(self, node, keys):
        if node.IT in self.mapping_table:
            return self.processMapping(self._OPCODE_UPDATE, node.type, node, keys)
        else:
            return None

    def removeMapping(self, id):

        (node, data) = self.table[id]
        type = node.type


        if node.IT in self.mapping_table:

            if not (path:= self.mapping_table[node.IT]):
                return None

            empty = True
            ni = 1
            while empty and len(path) > ni:

                try:
                    if not (found:= self.mapping_data.get_path(path[:-ni])) is None:

                        for item in found:
                            if isinstance(item, dict):
                                empty = False
                                break

                        if empty or ni == 1:
                            found.pop(path[-ni])

                except:
                    pass

                ni+= 1

            del(self.mapping_table[node.IT])

            return [(self._OPCODE_DELETE, path, None, None)]

        return None

    def insertTreeNode(self, type, name, parent, data):

        params = self.parseParamsData(type, data)
        node = AnyNode(IT=name, parent=parent, type=type, **params)

        self.table[data['IT']] = [node, data]

        if not parent is None:

            if type in [self.TYPE_PARTICIPANT, self.TYPE_MARKET] and node.parent.type in [self.TYPE_MARKET, self.TYPE_MARKET_GROUP]:
                try:
                    childrens = list(node.parent.children)
                    childrens.sort(key=lambda x: x.order)
                    node.parent.children = childrens
                except:
                    pass
            return self.insertMapping(type, node, params.keys())

        return (node, params.keys())

    def updateTreeNode(self, id, data):

        if id in self.table:

            node = self.table[id][0]

            #diff_result = diff(dictfilt(self.table[id][1], data.keys()), data)
            #patch(diff_result, self.table[id][1], True)
            if data is not None:
                self.table[id][1] = {**self.table[id][1], **data}

            params = self.parseParamsData(node.type, self.table[id][1])
            diff_result = diff(self.dictfilt(node.__dict__, params.keys()), params)
            #patch(diff_result, node.__dict__, True)
            node.__dict__.update(params)

            return self.updateMapping(node, [v[1] for v in diff_result])

        return None

    def removeTreeNode(self, id):

        if id in self.table:
            node = self.table[id][0]

            for child in node.descendants:
                del(child)

            path = self.removeMapping(id)

            node.parent = None
            del(node)

            del(self.table[id])

            return path

        return None

    def insertData(self, id, type, data):

        (node_parent_name, node_name) = id.split(self._PATH_DELIM)

        if node_parent_name in self.table:
            node_parent = self.table[node_parent_name][0]

            return self.insertTreeNode(type, node_name, node_parent, data)

        return None

    def updateData(self, id, data):

        return self.updateTreeNode(id, data)

    def removeData(self, id):

        node_name = id.split(self._PATH_DELIM)[-1]
        return self.removeTreeNode(node_name)

    def parseHierarchyInit(self, id):

        self.tree_root = AnyNode(IT=id)
        self.tree_state = [(None, self.tree_root)]
        self.table[id] = [self.tree_root, None]

    def parseHierarchyProcess(self, type, params):
        if type not in self._TYPE_HIERARCHY:
            return False

        (node, keys) = self.insertTreeNode(type, params['IT'], None, params)

        if type != self.tree_state[-1][0]:
            if not any(type == v[0] for v in self.tree_state):
                parents = self._TYPE_HIERARCHY[type]
                if parents is not None:
                    state = None
                    for parent in parents:
                        index = next((index for (index, v) in enumerate(self.tree_state) if v[0] == parent), None)
                        if index is not None:
                            state = index
                            break
                    if state is not None:
                        del self.tree_state[state + 1:]
            else:
                state = next((index for (index, v) in enumerate(self.tree_state) if v[0] == type), None)
                del self.tree_state[state:]

            self.tree_state.append((type, node))

        node.parent = self.tree_state[-2][1]

        return self.insertMapping(type, node, keys, True)

    def parseHierarchyFree(self):

        self.tree_root.parent = self.tree_data
        del self.tree_state

        if len(self.mapping_cache):
            pd = PathDict({})
            min_path = self.mapping_cache[0][0]
            events = []
            for item in self.mapping_cache:
                (path, data, node) = item
                if node.type == self.TYPE_EVENT and node.IT.startswith('OV'):
                    events.append(node)
                value = pd.get_path(path)
                if isinstance(value, dict):
                    value.update(data)
                else:
                    pd.set_path(deepcopy(path), data)
                if len(path) < len(min_path):
                    min_path = path

            while min_path:
                 del min_path[-1]
                 if not pd.get_path(min_path).__len__:
                     break

            self.mapping_cache.clear()
            return (min_path, pd.get_path(min_path).dict, events)

        return None

    def parseOVInPlay(self, type, params):
        return self.insertData(type, params)

    def parseConfig(self, type, params):
        pass

    def parseEmpty(self, type, params):
        pass

    def getSubscription(self, node, subscribe = True):
        subscribe_id = 1 if subscribe else 3
        return [
                '6V{}C{}A_1_{}'.format(node.sid, node.cid, subscribe_id),
                '{}{}{}{}M{}_{}'.format(node.c1, node.t1, node.c2, node.t2, node.cid, subscribe_id)
            ]

    def parseParams(self, data):
        chunks = list(filter(None, data.split(self._FIELD_DELIM)))
        type = chunks.pop(0) if len(chunks) and self._KEYVALUE_DELIM not in chunks[0] else None

        return (type, dict(map(lambda x: tuple(x.split(self._KEYVALUE_DELIM)), chunks)) if len(chunks) else None)


    def parseRecords(self, data):
        for record in deque(data.split(self._RECORD_DELIM)[:-1]):
            yield self.parseParams(record)

    def parseMessage(self, msg):

        id = msg.topic
        data = msg.body

        if msg.type in [self._TYPES_TOPIC_LOAD_MESSAGE, self._TYPES_DELTA_MESSAGE] \
            and (id.startswith(('OV', '6V', 'ML', 'OVInPlay', '__time', 'CONFIG', 'EMPTY')) or re.match(self.RE_MEDIA, id)) \
            and not id.startswith('OVM'):

            records = self.parseRecords(data)
            with suppress(StopIteration):
                if (opcode:= next(records)[0]):

                    if id == '__time':
                        if opcode in [self._OPCODE_INIT, self._OPCODE_UPDATE]:
                            yield ServerTime(self.parseServerTime(*next(records)))

                    if msg.type == self._TYPES_TOPIC_LOAD_MESSAGE:

                        if id.startswith('OVInPlay') or id.startswith('6V') or re.match(self.RE_MEDIA, id):

                            if id.startswith('OVInPlay'):
                                yield OvInPlay()

                            self.parseHierarchyInit(id)
                            for (type, params) in records:
                                self.parseHierarchyProcess(type, params)
                            if (result:= self.parseHierarchyFree()):
                                (path, data, events) = result
                                yield InsertData(id, path, data)

                                for event in events:
                                    yield Subscription(self.getSubscription(event))

                    if msg.type == self._TYPES_DELTA_MESSAGE:

                        for (type, params) in records:

                            ops = None
                            if opcode == self._OPCODE_INSERT or opcode == self._OPCODE_INIT:
                                ops = self.insertData(id, type, params)

                            elif opcode == self._OPCODE_UPDATE:

                                #if id.startswith('OV') or id.startswith('6V'):
                                    ops = self.updateData(id, params)

                            elif opcode == self._OPCODE_DELETE:
                                ops = self.removeData(id)

                            if ops is not None:
                                for op in ops:
                                    (code, path, data, node) = op
                                    if code == self._OPCODE_INSERT:
                                        yield InsertData(id, path, data)

                                        if node.type == self.TYPE_EVENT:
                                            yield Subscription(self.getSubscription(node))

                                    elif code == self._OPCODE_UPDATE:
                                        yield UpdateData(id, path, data)

                                    elif code == self._OPCODE_DELETE:
                                        yield RemoveData(id, path)


    @classmethod
    def parseAuth(self, msg):
        records = msg.split(self._DELIMITERS_RECORD)
        if records[0][1:] == self._DELIMITERS_HANDSHAKE+'P':
            record = records[1]
            return record[record.find('S_')+2:record.find(self._ENCODINGS_NONE)].split(self._AUTH_TOKEN_DELIM)
        return False

    def onMessage(self, data):

        messages = deque(data.split(self._DELIMITERS_MESSAGE))

        while len(messages):
            msg = messages.popleft()

            if not len(msg):
                continue

            type = msg[0]

            if type == self._TYPES_TOPIC_STATUS_NOTIFICATION:
                if (result:= self.parseAuth()):
                    (session_id, nst_token) = result
                    yield Auth(session_id, nst_token)
                continue

            if type == self._TYPES_SUBSCRIBE:
                record = msg.split(self._DELIMITERS_RECORD)[0]
                if record[1] == self._ENCODINGS_NONE:
                    topics = record[2:].split(self._TOPICS_DELIM)
                    yield Subscription(topics)
                continue

            if type in [self._TYPES_TOPIC_LOAD_MESSAGE, self._TYPES_DELTA_MESSAGE]:
                records = msg.split(self._DELIMITERS_RECORD)
                chunks = records[0].split(self._DELIMITERS_FIELD)
                topic = chunks[0][1:]
                body = msg[(len(records[0]) + 1):]
                yield Message(type, topic, body)
                continue

            if msg.startswith(u'100'):
                fields = msg.split(self._DELIMITERS_FIELD)
                yield M100(fields[1])
                continue

            if msg[:2] == self._DELIMITERS_FIELD+self._ENCODINGS_NONE:
                records = msg.split(self._DELIMITERS_RECORD)
                if records[0][2:] == 'command':
                    fields = records[2].split(self._DELIMITERS_FIELD)
                    yield Command(records[1], fields)
                    continue

