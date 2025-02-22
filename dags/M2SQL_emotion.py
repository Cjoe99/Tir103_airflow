from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import pymongo
import mysql.connector
import jieba
import re
import numpy as np
import logging
from pymongo import MongoClient
from mysql.connector import Error
from collections import Counter, defaultdict
from dataclasses import dataclass
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple
from functools import lru_cache

# 定義情緒詞典
EMOTION_DICT = {
    # 喜悅
    '開心': '喜悅', '快樂': '喜悅', '高興': '喜悅', '興奮': '喜悅',
    '愉快': '喜悅', '歡喜': '喜悅', '開朗': '喜悅', '暢快': '喜悅',
    '歡樂': '喜悅', '笑': '喜悅', '微笑': '喜悅', '大笑': '喜悅',
    '欣喜': '喜悅', '欣慰': '喜悅', '喜悅': '喜悅', '喜氣洋洋': '喜悅',
    '眉開眼笑': '喜悅', '樂呵呵': '喜悅', '樂滋滋': '喜悅', '喜形於色': '喜悅',
    '心花怒放': '喜悅', '春風得意': '喜悅', '得意洋洋': '喜悅', '歡天喜地': '喜悅',
    '興高采烈': '喜悅', '喜氣盈門': '喜悅', '笑逐顏開': '喜悅', '笑容滿面': '喜悅',
    '喜笑顏開': '喜悅', '欣欣然': '喜悅', '雀躍': '喜悅', '飛揚': '喜悅',
    '歡欣': '喜悅', '歡快': '喜悅', '愉悅': '喜悅', '喜不自勝': '喜悅',
    '欣喜若狂': '喜悅', '樂不可支': '喜悅', '笑嘻嘻': '喜悅', '眉飛色舞': '喜悅',
    '心曠神怡': '喜悅', '舒暢': '喜悅', '愉快': '喜悅', '歡愉': '喜悅',
    '喜氣': '喜悅', '喜慶': '喜悅', '歡暢': '喜悅', '怡然': '喜悅',


    # 安心
    '放心': '安心', '安心': '安心', '踏實': '安心', '安穩': '安心',
    '平安': '安心', '安定': '安心', '安寧': '安心', '鎮定': '安心',
    '穩重': '安心', '沉著': '安心', '泰然': '安心', '安詳': '安心',
    '安泰': '安心', '安然': '安心', '安適': '安心', '安逸': '安心',
    '安祥': '安心', '安和': '安心', '安樂': '安心', '安靜': '安心',
    '安分': '安心', '安居': '安心', '安度': '安心', '安妥': '安心',
    '安恬': '安心', '安好': '安心', '安康': '安心', '安全': '安心',
    '安生': '安心', '安身': '安心', '安土': '安心', '安養': '安心',
    '安泊': '安心', '安置': '安心', '安眠': '安心', '安歇': '安心',
    '安枕': '安心', '安然無恙': '安心', '安之若素': '安心', '安身立命': '安心',
    '安營扎寨': '安心', '安營紮寨': '安心', '安步當車': '安心', '安居樂業': '安心',
    '安堵': '安心', '安頓': '安心', '安排': '安心', '安息': '安心',


    # 期待
    '期待': '期待', '期盼': '期待', '盼望': '期待', '希望': '期待',
    '憧憬': '期待', '展望': '期待', '嚮往': '期待', '企盼': '期待',
    '冀望': '期待', '期許': '期待', '期望': '期待', '渴望': '期待',
    '企望': '期待', '盼': '期待', '盼念': '期待', '守望': '期待',
    '期求': '期待', '期念': '期待', '翹首': '期待', '翹盼': '期待',
    '翹企': '期待', '翹望': '期待', '瞻望': '期待', '盼顧': '期待',
    '盼佇': '期待', '期凝': '期待', '期盼': '期待', '期待': '期待',
    '望眼欲穿': '期待', '翹首以待': '期待', '引領期盼': '期待', '盼星星盼月亮': '期待',
    '望穿秋水': '期待', '殷切期待': '期待', '引頸期待': '期待', '期期艾艾': '期待',
    '期頤': '期待', '期勉': '期待', '期約': '期待', '期待': '期待',
    '盼顧': '期待', '盼望': '期待', '盼歸': '期待', '盼著': '期待',


    # 驚訝
    '驚訝': '驚訝', '震驚': '驚訝', '吃驚': '驚訝', '詫異': '驚訝',
    '意外': '驚訝', '驚奇': '驚訝', '驚詫': '驚訝', '愕然': '驚訝',
    '驚愕': '驚訝', '驚異': '驚訝', '驚歎': '驚訝', '讚歎': '驚訝',
    '驚嘆': '驚訝', '驚恐': '驚訝', '驚懼': '驚訝', '驚慌': '驚訝',
    '驚惶': '驚訝', '驚惶失措': '驚訝', '驚慌失措': '驚訝', '大吃一驚': '驚訝',
    '目瞪口呆': '驚訝', '瞠目結舌': '驚訝', '駭然': '驚訝', '震駭': '驚訝',
    '愣住': '驚訝', '怔住': '驚訝', '呆住': '驚訝', '嚇': '驚訝',
    '嚇一跳': '驚訝', '錯愕': '驚訝', '震悚': '驚訝', '咋舌': '驚訝',
    '詫': '驚訝', '駭': '驚訝', '愣': '驚訝', '怔': '驚訝',
    '嘖嘖稱奇': '驚訝', '嘖嘖稱異': '驚訝', '驚為天人': '驚訝', '瞪目結舌': '驚訝',
    '瞠目': '驚訝', '愣神': '驚訝', '發愣': '驚訝', '發怔': '驚訝',


    # 生氣
    '生氣': '生氣', '憤怒': '生氣', '火大': '生氣', '惱火': '生氣',
    '暴怒': '生氣', '氣憤': '生氣', '惱怒': '生氣', '發飆': '生氣',
    '動怒': '生氣', '發怒': '生氣', '發火': '生氣', '惱羞': '生氣',
    '光火': '生氣', '氣沖沖': '生氣', '怒氣沖天': '生氣', '怒髮衝冠': '生氣',
    '氣急敗壞': '生氣', '氣憤填膺': '生氣', '怒不可遏': '生氣', '暴跳如雷': '生氣',
    '火冒三丈': '生氣', '大發雷霆': '生氣', '怒氣衝衝': '生氣', '氣惱': '生氣',
    '惱': '生氣', '怒': '生氣', '憤': '生氣', '怨': '生氣',
    '怨恨': '生氣', '憎': '生氣', '憤恨': '生氣', '憤憤': '生氣',
    '憤然': '生氣', '憤慨': '生氣', '憤懣': '生氣', '憎恨': '生氣',
    '氣炸': '生氣', '氣瘋': '生氣', '氣死': '生氣', '氣壞': '生氣',
    '氣急': '生氣', '氣極': '生氣', '氣憋': '生氣', '氣悶': '生氣',


    # 討厭
    '討厭': '討厭', '厭惡': '討厭', '煩': '討厭', '嫌惡': '討厭',
    '反感': '討厭', '憎惡': '討厭', '厭煩': '討厭', '嫌棄': '討厭',
    '厭倦': '討厭', '厭棄': '討厭', '討人厭': '討厭', '嫌': '討厭',
    '嫌棄': '討厭', '嫌惡': '討厭', '嫌厭': '討厭', '討嫌': '討厭',
    '煩躁': '討厭', '煩悶': '討厭', '煩心': '討厭', '煩惱': '討厭',
    '厭世': '討厭', '厭煩': '討厭', '厭惡': '討厭', '厭棄': '討厭',
    '厭憎': '討厭', '厭倦': '討厭', '厭膩': '討厭', '厭棄': '討厭',
    '憎恨': '討厭', '憎惡': '討厭', '憎嫌': '討厭', '憎厭': '討厭',
    '反胃': '討厭', '反感': '討厭', '反對': '討厭', '反惡': '討厭',
    '嫌棄': '討厭', '嫌厭': '討厭', '嫌惡': '討厭', '嫌憎': '討厭',
    '討厭': '討厭', '討人嫌': '討厭', '討人厭': '討厭', '討不喜': '討厭',


    # 難過
    '難過': '難過', '傷心': '難過', '悲傷': '難過', '哀傷': '難過',
    '悲痛': '難過', '痛心': '難過', '哭泣': '難過', '心疼': '難過',
    '悲': '難過', '哀': '難過', '愁': '難過', '憂': '難過',
    '苦': '難過', '慟': '難過', '痛': '難過', '傷': '難過',
    '悲哀': '難過', '悲痛': '難過', '悲慘': '難過', '悲涼': '難過',
    '悲戚': '難過', '悲憤': '難過', '悲憫': '難過', '悲泣': '難過',
    '悲切': '難過', '悲咽': '難過', '悲愴': '難過', '悲嘆': '難過',
    '悲鳴': '難過', '悲慟': '難過', '悲怨': '難過', '悲苦': '難過',
    '痛哭': '難過', '痛苦': '難過', '痛楚': '難過', '痛悔': '難過',
    '心酸': '難過', '心痛': '難過', '心碎': '難過', '心傷': '難過',
    '黯然': '難過', '黯淡': '難過', '慘然': '難過', '慘澹': '難過',


    # 焦慮
    '焦慮': '焦慮', '擔心': '焦慮', '憂慮': '焦慮', '不安': '焦慮',
    '緊張': '焦慮', '慌張': '焦慮', '惶恐': '焦慮', '憂心': '焦慮',
    '忐忑': '焦慮', '惴惴': '焦慮', '煎熬': '焦慮', '憂懼': '焦慮',
    '憂愁': '焦慮', '憂悶': '焦慮', '憂心忡忡': '焦慮', '憂心如焚': '焦慮',
    '忐忑不安': '焦慮', '坐立不安': '焦慮', '心神不寧': '焦慮', '心慌意亂': '焦慮',
    '心急如焚': '焦慮',

    # 正面
    '支持': '正面', '自由': '正面', '希望': '正面', '喜歡': '正面',
    '相信': '正面', '不錯': '正面', '同意': '正面', '嘻嘻': '正面',
    '贊同': '正面', '認可': '正面', '肯定': '正面', '讚賞': '正面',
    '欣賞': '正面', '贊成': '正面', '認同': '正面', '讚同': '正面',
    '支援': '正面', '支撐': '正面', '支援': '正面', '支持者': '正面',
    '擁護': '正面', '維護': '正面', '擁戴': '正面', '贊助': '正面',
    '自在': '正面', '自主': '正面', '自如': '正面', '自由自在': '正面',
    '解放': '正面', '開放': '正面', '無拘無束': '正面', '暢快': '正面',
    '嚮往': '正面', '期望': '正面', '期待': '正面', '冀望': '正面',
    '摯愛': '正面', '喜愛': '正面', '鍾愛': '正面', '愛慕': '正面',
    '篤信': '正面', '深信': '正面', '信任': '正面', '信賴': '正面',
    '優良': '正面', '良好': '正面', '優質': '正面', '出色': '正面',


    # 反感
    '噁心': '反感', '討厭': '反感', '不爽': '反感', '垃圾': '反感',
    '低能': '反感', '智障': '反感', '白痴': '反感', '白癡': '反感',
    '噁': '反感', '嫌惡': '反感', '厭惡': '反感', '反胃': '反感',
    '作嘔': '反感', '惡心': '反感', '嘔心': '反感', '嫌棄': '反感',
    '厭煩': '反感', '煩躁': '反感', '厭倦': '反感', '厭棄': '反感',
    '痛恨': '反感', '憎恨': '反感', '憎惡': '反感', '憎嫌': '反感',
    '反感': '反感', '反對': '反感', '抗議': '反感', '抵制': '反感',
    '嫌棄': '反感', '嫌厭': '反感', '嫌惡': '反感', '嫌憎': '反感',
    '排斥': '反感', '排擠': '反感', '排除': '反感', '驅逐': '反感',
    '唾棄': '反感', '唾罵': '反感', '咒罵': '反感', '辱罵': '反感',

    # 失望
    '可憐': '失望', '可悲': '失望', '不好': '失望', '失敗': '失望',
    '擔心': '失望', '隨便': '失望', '沮喪': '失望', '灰心': '失望',
    '喪氣': '失望', '泄氣': '失望', '氣餒': '失望', '頹喪': '失望',
    '失意': '失望', '消沉': '失望', '低落': '失望', '頹廢': '失望',
    '失落': '失望', '挫敗': '失望', '挫折': '失望', '打擊': '失望',
    '墮落': '失望', '沉淪': '失望', '沉淪': '失望', '萎靡': '失望',
    '衰敗': '失望', '衰退': '失望', '衰落': '失望', '衰微': '失望',
    '頹唐': '失望', '頹廢': '失望', '頹靡': '失望', '頹勢': '失望',
    '失望': '失望', '失意': '失望', '失落': '失望', '失志': '失望',
    '悲觀': '失望', '消極': '失望', '頹喪': '失望', '低迷': '失望',

    # 違規
#   '違規': '違規', '違法': '違規', '問題': '違規', '八卦': '違規',
#   '犯規': '違規', '違反': '違規', '觸法': '違規', '犯法': '違規',
#   '越軌': '違規', '違紀': '違規', '違章': '違規', '違例': '違規',
#   '違禁': '違規', '違約': '違規', '違背': '違規', '違逆': '違規',
#   '違抗': '違規', '違令': '違規', '違命': '違規', '違制': '違規',
#   '違犯': '違規', '違反': '違規', '違背': '違規', '違逆': '違規',
#   '違拗': '違規', '違悖': '違規', '違忤': '違規', '違諫': '違規',
#   '犯禁': '違規', '犯戒': '違規', '犯例': '違規', '犯規': '違規',
#   '越權': '違規', '越位': '違規', '越矩': '違規', '越界': '違規',
#   '觸犯': '違規', '觸法': '違規', '觸例': '違規', '觸規': '違規',

    # 貶低
    '白癡': '貶低', '智障': '貶低', '低能': '貶低', '白痴': '貶低',
    '笨蛋': '貶低', '蠢貨': '貶低', '廢物': '貶低', '無能': '貶低',
    '腦殘': '貶低', '蠢材': '貶低', '呆子': '貶低', '傻瓜': '貶低',
    '笨豬': '貶低', '蠢豬': '貶低', '笨蛋': '貶低', '笨拙': '貶低',
    '愚笨': '貶低', '愚蠢': '貶低', '愚昧': '貶低', '愚鈍': '貶低',
    '愚钝': '貶低', '愚魯': '貶低', '愚痴': '貶低', '愚懵': '貶低',
    '遲鈍': '貶低', '魯鈍': '貶低', '糊塗': '貶低', '癡呆': '貶低',
    '無知': '貶低', '無腦': '貶低', '沒腦': '貶低', '缺腦': '貶低',
    '蠢笨': '貶低', '蠢鈍': '貶低', '蠢拙': '貶低', '蠢態': '貶低',

    # 憤怒
    '生氣': '憤怒', '憤怒': '憤怒', '惱火': '憤怒', '暴怒': '憤怒',
    '火大': '憤怒', '發飆': '憤怒', '發火': '憤怒', '發怒': '憤怒',
    '氣憤': '憤怒', '氣惱': '憤怒', '氣炸': '憤怒', '氣瘋': '憤怒',
    '暴跳': '憤怒', '狂怒': '憤怒', '大怒': '憤怒', '震怒': '憤怒',
    '怒不可遏': '憤怒', '怒髮衝冠': '憤怒', '怒火中燒': '憤怒', '怒氣沖天': '憤怒',
    '火冒三丈': '憤怒', '大發雷霆': '憤怒', '雷霆之怒': '憤怒', '暴跳如雷': '憤怒',
    '氣急敗壞': '憤怒', '氣憤填膺': '憤怒', '怒形於色': '憤怒', '怒氣衝衝': '憤怒',
    '憤憤不平': '憤怒', '憤慨': '憤怒', '憤恨': '憤怒', '憤然': '憤怒',

    # 厭惡
    '討厭': '厭惡', '噁心': '厭惡', '厭煩': '厭惡', '厭惡': '厭惡',
    '嫌惡': '厭惡', '反感': '厭惡', '排斥': '厭惡', '抵制': '厭惡',
    '嫌棄': '厭惡', '嫌厭': '厭惡', '嫌憎': '厭惡', '厭棄': '厭惡',
    '憎惡': '厭惡', '憎恨': '厭惡', '痛恨': '厭惡', '可惡': '厭惡',
    '可憎': '厭惡', '可惡': '厭惡', '可厭': '厭惡', '可恨': '厭惡',
    '作嘔': '厭惡', '反胃': '厭惡', '嘔心': '厭惡', '惡心': '厭惡',
    '腥臭': '厭惡', '骯髒': '厭惡', '污穢': '厭惡', '齷齪': '厭惡',
    '骯髒': '厭惡', '污濁': '厭惡', '髒亂': '厭惡', '汙穢': '厭惡',

    # 質疑
    '質疑': '質疑', '懷疑': '質疑', '存疑': '質疑', '疑問': '質疑',
    '不信': '質疑', '狐疑': '質疑', '疑慮': '質疑', '疑竇': '質疑',
    '困惑': '質疑', '不解': '質疑', '納悶': '質疑', '費解': '質疑',
    '不懂': '質疑', '不確定': '質疑', '不肯定': '質疑',
    '半信半疑': '質疑', '將信將疑': '質疑', '疑神疑鬼': '質疑', '疑雲': '質疑',
    '疑點': '質疑', '疑案': '質疑', '疑團': '質疑', '疑陣': '質疑',
    '不可信': '質疑', '不可靠': '質疑', '不可取': '質疑', '不可信賴': '質疑',
    '似是而非': '質疑', '模稜兩可': '質疑', '曖昧不明': '質疑', '撲朔迷離': '質疑',


    # 諷刺
    '諷刺': '諷刺', '嘲諷': '諷刺', '譏諷': '諷刺', '揶揄': '諷刺',
    '嘲笑': '諷刺', '訕笑': '諷刺', '譏笑': '諷刺', '恥笑': '諷刺',
    '冷嘲': '諷刺', '熱諷': '諷刺', '嘲弄': '諷刺', '戲弄': '諷刺',
    '譏刺': '諷刺', '譏評': '諷刺', '譏誚': '諷刺', '譏彈': '諷刺',
    '挖苦': '諷刺', '奚落': '諷刺', '嘲謔': '諷刺', '調侃': '諷刺',
    '訕笑': '諷刺', '訕謗': '諷刺', '訕評': '諷刺', '訕刺': '諷刺',
    '譏評': '諷刺', '貶損': '諷刺', '貶抑': '諷刺', '貶低': '諷刺',
    '嘲弄': '諷刺', '戲謔': '諷刺', '取笑': '諷刺', '笑話': '諷刺',

    # 不滿
    '不滿': '不滿', '不爽': '不滿', '不悅': '不滿', '不快': '不滿',
    '不服': '不滿', '不甘': '不滿', '不平': '不滿', '不願': '不滿',
    '不樂': '不滿', '不安': '不滿', '不適': '不滿', '不舒': '不滿',
    '抱怨': '不滿', '怨言': '不滿', '怨聲': '不滿', '怨氣': '不滿',
    '埋怨': '不滿', '牢騷': '不滿', '怨懟': '不滿', '怨恨': '不滿',
    '不悅': '不滿', '不樂': '不滿', '不快': '不滿', '不適': '不滿',
    '不舒服': '不滿', '不自在': '不滿', '不痛快': '不滿', '不舒坦': '不滿',
    '憤憤不平': '不滿', '憤懣': '不滿', '憤慨': '不滿', '怏怏不樂': '不滿',

    # 擔憂
    '擔心': '擔憂', '憂慮': '擔憂', '擔憂': '擔憂', '憂心': '擔憂',
    '焦慮': '擔憂', '焦急': '擔憂', '憂急': '擔憂', '憂悶': '擔憂',
    '憂愁': '擔憂', '憂傷': '擔憂', '憂鬱': '擔憂', '憂煩': '擔憂',
    '憂慮重重': '擔憂', '憂心忡忡': '擔憂', '憂心如焚': '擔憂', '憂思': '擔憂',
    '掛心': '擔憂', '掛慮': '擔憂', '牽掛': '擔憂', '介懷': '擔憂',
    '放心不下': '擔憂', '寢食難安': '擔憂', '坐立不安': '擔憂', '心神不寧': '擔憂',
    '憂愁': '擔憂', '憂悶': '擔憂', '憂傷': '擔憂', '憂鬱': '擔憂',
    '憂懼': '擔憂', '憂慮': '擔憂', '憂思': '擔憂', '憂煩': '擔憂',

    # 支持
    '支持': '支持', '贊同': '支持', '同意': '支持', '認可': '支持',
    '贊成': '支持', '支援': '支持', '擁護': '支持', '支撐': '支持',
    '支援': '支持', '支助': '支持', '支應': '支持', '支持者': '支持',
    '贊助': '支持', '贊助者': '支持', '贊同者': '支持', '支持者': '支持',
    '擁戴': '支持', '擁護': '支持', '擁立': '支持', '擁戴者': '支持',
    '認同': '支持', '認可': '支持', '認許': '支持', '認證': '支持',
    '贊許': '支持', '贊可': '支持', '贊揚': '支持', '贊美': '支持',
    '支持': '支持', '支援': '支持', '支助': '支持', '支應': '支持',

    # 後悔
    '後悔': '後悔', '懊悔': '後悔', '悔恨': '後悔', '遺憾': '後悔',
    '懊惱': '後悔', '悔不當初': '後悔', '追悔莫及': '後悔', '悔之晚矣': '後悔',
    '悔青': '後悔', '懊喪': '後悔', '自責': '後悔', '懺悔': '後悔',
    '痛悔': '後悔', '悔改': '後悔', '悔意': '後悔', '追悔': '後悔',
    '後悔莫及': '後悔', '悔不該': '後悔', '悔錯': '後悔', '悔過': '後悔',
    '悔恨交加': '後悔', '悔之不及': '後悔', '追悔末及': '後悔', '悔不絕': '後悔',
    '悔悟': '後悔', '悔恨不已': '後悔', '悔不堪言': '後悔', '追悔無及': '後悔',
    '悔之無及': '後悔', '追悔不及': '後悔', '懊惱不已': '後悔', '悔恨萬分': '後悔',
    '悔不迭': '後悔', '追悔不已': '後悔', '懊惱不堪': '後悔', '懺悔不已': '後悔',

    # 麻煩
#   '麻煩': '麻煩', '困擾': '麻煩', '棘手': '麻煩', '難辦': '麻煩',
#   '費事': '麻煩', '累贅': '麻煩', '添亂': '麻煩', '攪擾': '麻煩',
#   '勞煩': '麻煩', '為難': '麻煩', '費神': '麻煩', '費力': '麻煩',
#   '費時': '麻煩', '勞師動眾': '麻煩', '添麻煩': '麻煩', '惹麻煩': '麻煩',
#   '找麻煩': '麻煩', '製造麻煩': '麻煩', '帶來麻煩': '麻煩', '出狀況': '麻煩',
#   '出問題': '麻煩', '徒增麻煩': '麻煩', '添堵': '麻煩', '費周章': '麻煩',
#   '勞駕': '麻煩', '勞累': '麻煩', '打擾': '麻煩', '困境': '麻煩',
#   '難處': '麻煩', '艱難': '麻煩', '繁瑣': '麻煩', '複雜': '麻煩',
#   '費工夫': '麻煩', '費勁': '麻煩', '費心': '麻煩', '費功夫': '麻煩',

    # 成功
    '成功': '成功', '達成': '成功', '實現': '成功', '完成': '成功',
    '做到': '成功', '成就': '成功', '贏得': '成功', '征服': '成功',
    '突破': '成功', '得勝': '成功', '成功率': '成功', '告捷': '成功',
    '大功告成': '成功', '功德圓滿': '成功', '圓滿': '成功', '順利': '成功',
    '達標': '成功', '成效': '成功', '建樹': '成功', '創舉': '成功',
    '突破性': '成功', '里程碑': '成功', '佳績': '成功', '捷報': '成功',
    '創下紀錄': '成功', '成果': '成功', '斬獲': '成功', '勝利': '成功',
    '獲勝': '成功', '奪冠': '成功', '稱霸': '成功', '成名': '成功',
    '出人頭地': '成功', '平步青雲': '成功', '登峰造極': '成功', '揚名立萬': '成功',

    # 謝謝
    '謝謝': '謝謝', '感謝': '謝謝', '感恩': '謝謝', '多謝': '謝謝',
    '致謝': '謝謝', '謝意': '謝謝', '感激': '謝謝', '感念': '謝謝',
    '謝天謝地': '謝謝', '萬分感謝': '謝謝', '銘謝': '謝謝', '道謝': '謝謝',
    '答謝': '謝謝', '謝禮': '謝謝', '謝忱': '謝謝', '謝函': '謝謝',
    '謝卡': '謝謝', '謝詞': '謝謝', '謝信': '謝謝', '感恩戴德': '謝謝',
    '感激不盡': '謝謝', '感激涕零': '謝謝', '感謝不盡': '謝謝', '感恩圖報': '謝謝',
    '感激萬分': '謝謝', '感激不已': '謝謝', '感激莫名': '謝謝', '銘感五內': '謝謝',
    '感激之情': '謝謝', '感謝之意': '謝謝', '謝意難盡': '謝謝', '感激之至': '謝謝',
    '感恩之心': '謝謝', '感激之心': '謝謝', '感謝之心': '謝謝', '謝意難表': '謝謝',

    # 優惠
    '優惠': '優惠', '特價': '優惠', '折扣': '優惠', '促銷': '優惠',
    '減價': '優惠', '便宜': '優惠', '划算': '優惠', '優待': '優惠',
    '優勢': '優惠', '實惠': '優惠', '優惠券': '優惠', '折價券': '優惠',
    '特惠': '優惠', '優惠價': '優惠', '限時優惠': '優惠', '折扣價': '優惠',
    '特賣': '優惠', '讓利': '優惠', '折讓': '優惠', '優惠方案': '優惠',
    '優惠活動': '優惠', '折價': '優惠', '減免': '優惠', '促銷價': '優惠',
    '特價品': '優惠', '優待券': '優惠', '回饋': '優惠', '優惠卷': '優惠',
    '特惠價': '優惠', '優惠專案': '優惠', '優惠期間': '優惠', '促銷活動': '優惠',
    '打折': '優惠', '降價': '優惠', '特價優惠': '優惠', '限時特價': '優惠',

    # 有趣
    '有趣': '有趣', '有意思': '有趣', '好玩': '有趣', '有意思': '有趣',
    '逗趣': '有趣', '趣味': '有趣', '妙趣': '有趣', '風趣': '有趣',
    '幽默': '有趣', '搞笑': '有趣', '有梗': '有趣', '趣味性': '有趣',
    '饒富趣味': '有趣', '富趣味': '有趣', '趣味橫生': '有趣', '趣味盎然': '有趣',
    '饒富意趣': '有趣', '趣味十足': '有趣', '趣味無窮': '有趣', '妙趣橫生': '有趣',
    '趣味性強': '有趣', '趣味豐富': '有趣', '趣味無限': '有趣', '趣味盎然': '有趣',
    '妙不可言': '有趣', '妙趣無窮': '有趣', '趣味橫溢': '有趣', '趣味無窮': '有趣',
    '趣味盎然': '有趣', '妙趣橫生': '有趣', '趣味性強': '有趣', '趣味豐富': '有趣',
    '趣味無限': '有趣', '趣味十足': '有趣', '趣味橫生': '有趣', '趣味盎然': '有趣',

    # 願意
    '願意': '願意', '情願': '願意', '樂意': '願意', '甘願': '願意',
    '同意': '願意', '肯定': '願意', '答應': '願意', '首肯': '願意',
    '允許': '願意', '應允': '願意', '贊同': '願意', '認可': '願意',
    '默許': '願意', '准許': '願意', '批准': '願意', '准予': '願意',
    '應承': '願意', '應允': '願意', '贊成': '願意', '同意見': '願意',
    '允准': '願意', '同情': '願意', '贊許': '願意', '贊可': '願意',
    '認同': '願意', '承認': '願意', '承諾': '願意', '應許': '願意',
    '答應': '願意', '應諾': '願意', '樂於': '願意', '甘於': '願意',
    '願為': '願意', '樂為': '願意', '心甘情願': '願意', '甘之如飴': '願意'
}

# 情緒分類
EMOTION_CATEGORIES = {
    '喜悅': 1,    # 正面情緒
    '安心': 1,    # 正面情緒
    '期待': 1,    # 正面情緒
    '正面': 1,    # 正面情緒
    '支持': 1,    # 正面情緒
    '成功': 1,    # 正面情緒
    '謝謝': 1,    # 正面情緒
    '優惠': 1,    # 正面情緒
    '有趣': 1,    # 正面情緒
    '願意': 1,    # 正面情緒
    '驚訝': 0,    # 中性情緒
    '生氣': -1,   # 負面情緒
    '討厭': -1,   # 負面情緒
    '難過': -1,   # 負面情緒
    '焦慮': -1,   # 負面情緒
    '反感': -1,   # 負面情緒
    '失望': -1,   # 負面情緒
    '違規': -1,   # 負面情緒
    '貶低': -1,   # 負面情緒
    '憤怒': -1,   # 負面情緒
    '厭惡': -1,   # 負面情緒
    '質疑': -1,   # 負面情緒
    '諷刺': -1,   # 負面情緒
    '不滿': -1,   # 負面情緒
    '擔憂': -1,   # 負面情緒
    '後悔': -1,   # 負面情緒
    '麻煩': -1,   # 負面情緒
}

class DatabaseManager:
    """資料庫管理類別，包括MongoDB和MySQL的配置與操作"""

    MONGO_URI = "mongodb://XXXX:XXXX@IP:28017/admin"  # MongoDB連線字串
    MYSQL_CONFIG = {  # MySQL配置字典
        'host': 'IP',
        'database': 'PTT',
        'user': 'XXXX',
        'password': 'XXXX',
        'pool_size': 5,
        'pool_name': 'mypool',
        'buffered': True
    }
    BATCH_SIZE = 100  # 批次處理大小

    def __init__(self):
        """初始化方法，設定MongoDB客戶端和MySQL連線池"""
        self._mongo_client = None  # MongoDB客戶端
        self._mysql_pool = None  # MySQL連線池
        self._setup_logging()  # 設定日誌紀錄

    def _setup_logging(self):
        """配置日誌紀錄的處理器和格式"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('post_analyzer.log'),  # 寫入日誌檔案
                logging.StreamHandler()  # 也輸出到控制檯
            ]
        )

    @contextmanager
    def get_mongo_connection(self):
        """MongoDB連線的上下文管理器"""
        if not self._mongo_client:  # 檢查MongoDB客戶端是否已初始化
            try:
                self._mongo_client = MongoClient(DatabaseManager.MONGO_URI)  # 建立MongoDB連線
                db = self._mongo_client['kafka']  # 指定資料庫
                collection = db['merged_collection']  # 指定集合
                yield collection  # 輸出集合以供使用
            except Exception as e:
                logging.error(f"MongoDB連線錯誤: {e}")  # 紀錄錯誤資訊
                raise  # 引發例外
        else:
            yield self._mongo_client['kafka']['merged_collection']  # 使用已存在的MongoDB連線

    def _create_mysql_pool(self):
        """如果不存在，建立MySQL連線池"""
        if not self._mysql_pool:  # 檢查MySQL連線池是否已初始化
            try:
                self._mysql_pool = mysql.connector.pooling.MySQLConnectionPool(
                    **DatabaseManager.MYSQL_CONFIG  # 使用配置初始化連線池
                )
                logging.info("MySQL連線池建立成功")  # 紀錄成功訊息
            except Error as e:
                logging.error(f"建立MySQL連線池時出錯: {e}")  # 紀錄錯誤資訊
                raise  # 引發例外

    @contextmanager
    def get_mysql_connection(self):
        """MySQL連線的上下文管理器，使用連線池"""
        if not self._mysql_pool:  # 檢查是否需要建立MySQL連線池
            self._create_mysql_pool()  # 建立連線池

        connection = None  # 初始化連線變數
        try:
            connection = self._mysql_pool.get_connection()  # 從連線池獲取連線
            yield connection  # 輸出連線以供使用
        except Error as e:
            logging.error(f"從池中獲取MySQL連線時出錯: {e}")  # 紀錄錯誤資訊
            raise  # 引發例外
        finally:
            if connection:  # 確保連線被正確關閉
                connection.close()  # 關閉連線

class EmotionAnalyzer:
    """情緒分析類別，處理文本情緒分析和資料處理"""

    def __init__(self, db_manager: DatabaseManager, emotion_dict: dict, emotion_categories: dict):
        self.db_manager = db_manager
        self.emotion_dict = emotion_dict
        self.emotion_categories = emotion_categories
        self.processed_links = {}

    def format_date(self, date_string: str) -> Optional[str]:
        """統一日期格式為 YYYY-MM-DD"""
        if not date_string:
            return None

        try:
            # 處理新聞來源的年月日格式
            if '年' in date_string and '月' in date_string and '日' in date_string:
                date_parts = re.findall(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_string)
                if date_parts:
                    year, month, day = date_parts[0]
                    return f"{year}-{int(month):02d}-{int(day):02d}"

            # 處理PTT的月/日格式
            elif '/' in date_string and len(date_string.split('/')) == 2:
                month, day = map(int, date_string.split('/'))
                return f"2024-{month:02d}-{day:02d}"

            # 處理Dcard的ISO格式
            elif 'T' in date_string:
                return date_string.split('T')[0]

            # 處理純日期格式 (YYYY-MM-DD)
            elif re.match(r'\d{4}-\d{2}-\d{2}', date_string):
                return date_string

            return None

        except Exception as e:
            logging.error(f"日期解析錯誤: {e}, 日期字串: {date_string}")
            return None

    @staticmethod
    def _determine_source(url: str) -> str:
        """
        根據URL確定新聞來源
        Args: url: 新聞網址
        Returns: str: 新聞來源名稱
        """
        if not url:
            return 'ETtoday'

        url_mapping = {
            'ptt.cc': 'PTT',
            'dcard.tw': 'Dcard',
            'ettoday': 'ETtoday',
            'yahoo': 'Yahoo',
            'setn': '三立新聞網',
            'udn': '聯合新聞網',
            'ltn': '自由時報'
        }

        url_lower = url.lower()
        for key, value in url_mapping.items():
            if key in url_lower:
                return value
        return '其他新聞網'

    def analyze_emotions(self, text: str) -> Tuple[Dict[str, int], Dict[str, int]]:
        """分析文本中的情緒詞"""
        if not text:
            return defaultdict(int), defaultdict(int)

        words = jieba.lcut(text)
        emotion_word_counts = defaultdict(int)
        emotion_category_counts = defaultdict(int)

        for word in words:
            if word in self.emotion_dict:
                emotion_word_counts[word] += 1
                emotion_category_counts[self.emotion_dict[word]] += 1

        return emotion_word_counts, emotion_category_counts

    def _extract_content(self, document: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        從不同格式的文件中提取內容、日期和連結

        Args:
            document: MongoDB文件

        Returns:
            Tuple[Optional[str], Optional[str], Optional[str]]: (內容, 日期, 連結)
        """
        value = document.get('value', {})

        # 提取連結
        link = document.get('key')

        # 根據不同來源提取內容和日期
        if 'ptt.cc' in str(link):
            content = value.get('內容')
            date = value.get('發佈日期')
        elif 'dcard.tw' in str(link):
            content = value.get('內容')
            date = value.get('發布時間')
        else:  # ETtoday、Yahoo和其他新聞網站
            content = value.get('content')
            date = value.get('date')

        return content, date, link

    def process_collection(self) -> pd.DataFrame:
        """處理MongoDB集合中的文件並進行情緒分析"""
        results = []

        with self.db_manager.get_mongo_connection() as collection:
            cursor = collection.find()

            for document in cursor:
                try:
                    processed_data = self._process_document(document)
                    if processed_data:
                        results.append(processed_data)
                except Exception as e:
                    logging.error(f"處理文件時發生錯誤: {str(e)}")
                    continue

        return pd.DataFrame(results)

    def _process_document(self, document: dict) -> Optional[dict]:
        """處理單個文件的情緒分析"""
        try:
            # 提取基本資訊
            content, publish_date, link = self._extract_content(document)

            if not all([content, publish_date, link]):
                return None

            # 格式化日期
            formatted_date = self.format_date(publish_date)
            if not formatted_date:
                return None

            source = self._determine_source(link)

            # 分析情緒
            word_counts, category_counts = self.analyze_emotions(content)

            # 計算主要情緒和情緒分數
            dominant_emotion = max(category_counts.items(),
                                 key=lambda x: x[1])[0] if category_counts else None
            emotion_score = sum(count * self.emotion_categories[cat]
                              for cat, count in category_counts.items())

            return {
                "發佈日期": formatted_date,
                "來源": source,
                "情緒詞統計": dict(word_counts),
                "主要情緒": dominant_emotion,
                "情緒分數": emotion_score
            }

        except Exception as e:
            logging.error(f"處理文件時發生錯誤: {str(e)}")
            return None

    def save_to_mysql(self, df: pd.DataFrame):
        """
        將分析結果保存到MySQL資料庫

        Args:
            df: 包含分析結果的DataFrame
        """
        if df.empty:
            logging.warning("沒有資料要保存到MySQL")
            return

        drop_table_query = "DROP TABLE IF EXISTS emotion_analysis"

        create_table_query = """
        CREATE TABLE emotion_analysis (
            id INT AUTO_INCREMENT PRIMARY KEY,
            發佈日期 DATE,
            來源 VARCHAR(50),
            排名 INT,
            心情詞語 VARCHAR(50),
            次數 INT,
            主要情緒 VARCHAR(50),
            情緒分數 INT,
            UNIQUE KEY date_source_rank (發佈日期, 來源, 排名)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        with self.db_manager.get_mysql_connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(drop_table_query)
                cursor.execute(create_table_query)

                # 使用 REPLACE INTO 來處理重複資料
                insert_query = """
                REPLACE INTO emotion_analysis
                (發佈日期, 來源, 排名, 心情詞語, 次數, 主要情緒, 情緒分數)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                batch_data = []
                processed_keys = set()  # 用於追踪已處理的組合

                for _, row in df.iterrows():
                    emotion_words = row['情緒詞統計']
                    source = row['來源']
                    date = row['發佈日期']

                    # 排序情緒詞，確保相同日期和來源的文章有一致的排序
                    sorted_words = sorted(
                        emotion_words.items(),
                        key=lambda x: (-x[1], x[0])  # 先按次數降序，再按詞語字母順序
                    )

                    # 只取前15個最常出現的情緒詞
                    for rank, (word, count) in enumerate(sorted_words[:15], 1):
                        # 創建唯一鍵組合
                        key = (date, source, rank)

                        # 檢查是否已處理過這個組合
                        if key not in processed_keys:
                            batch_data.append((
                                date,
                                source,
                                rank,
                                word,
                                count,
                                row['主要情緒'],
                                row['情緒分數']
                            ))
                            processed_keys.add(key)

                        # 批次執行插入
                        if len(batch_data) >= 1000:
                            cursor.executemany(insert_query, batch_data)
                            batch_data = []
                            connection.commit()
                            logging.info(f"已批次處理 {len(processed_keys)} 筆資料")

                # 處理剩餘的資料
                if batch_data:
                    cursor.executemany(insert_query, batch_data)
                    connection.commit()
                    logging.info(f"資料處理完成，總共處理 {len(processed_keys)} 筆資料")

                logging.info("資料成功保存到MySQL")

            except Error as e:
                logging.error(f"保存到MySQL時發生錯誤: {e}")
                connection.rollback()
                raise

class MainProcessor:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.analyzer = EmotionAnalyzer(
            db_manager=self.db_manager,
            emotion_dict=EMOTION_DICT,
            emotion_categories=EMOTION_CATEGORIES
        )

    def process_and_analyze(self):
        logging.info("開始處理MongoDB資料...")
        results_df = self.analyzer.process_collection()
        
        if not results_df.empty:
            logging.info(f"成功處理 {len(results_df)} 筆資料")
            return results_df
        else:
            logging.warning("沒有找到可分析的資料")
            return None

    def save_results(self, results_df):
        if results_df is not None:
            logging.info("開始將結果儲存到MySQL...")
            self.analyzer.save_to_mysql(results_df)
            logging.info("情緒分析流程完成")
        else:
            logging.warning("沒有資料可以儲存")

# Airflow DAG 定義
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'emotion_analysis',
    default_args=default_args,
    description='每日情緒分析流程',
    schedule_interval='0 1 * * *',  # 每天凌晨1點執行
    catchup=False,
)

def init_processor():
    return MainProcessor()

def process_data(**context):
    processor = context['task_instance'].xcom_pull(task_ids='init_processor')
    results_df = processor.process_and_analyze()
    if results_df is not None:
        # 將 DataFrame 轉換為 JSON 字符串以便使用 XCom 傳遞
        return results_df.to_json()
    return None

def save_data(**context):
    results_json = context['task_instance'].xcom_pull(task_ids='process_data')
    if results_json:
        processor = context['task_instance'].xcom_pull(task_ids='init_processor')
        results_df = pd.read_json(results_json)
        processor.save_results(results_df)

# 定義任務
init_task = PythonOperator(
    task_id='init_processor',
    python_callable=init_processor,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag,
)

# 設定任務依賴關係
init_task >> process_task >> save_task