import codecs
import struct


def encode(raw_card):
    return codecs.encode(struct.pack("<I", int(raw_card)), 'hex').upper()
