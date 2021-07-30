
import re

from random import randint

def START_NODE():
    return randint(7*60, 8*60)

def SETUP_NODE():
    return randint(1*60, 2*60)

def CREATE_CONTAINER():
    return randint(11*60, 12*60)

def CLEAN_JOBS():
    return randint(0, 1*60)

def AHEAD_NOTIFY():
    return 120

def TOTAL():
    return START_NODE() + SETUP_NODE() + CREATE_CONTAINER()