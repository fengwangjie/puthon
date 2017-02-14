from os import urandom
import codecs

from pbkdf2 import PBKDF2


PBKDF2_ITERATIONS = 10
SALT_BYTE_SIZE = 10
HASH_BYTE_SIZE = 24

HASH_SECTIONS = 4
SECTION_DELIMITER = ':'
ITERATIONS_INDEX = 1
SALT_INDEX = 2
HASH_INDEX = 3


def _hexlify(b):
    return str(codecs.encode(b, 'hex'), 'utf-8')


def create_hash(password):
    salt = urandom(SALT_BYTE_SIZE)
    pbkdf2 = PBKDF2(password, salt, PBKDF2_ITERATIONS).read(HASH_BYTE_SIZE)
    return SECTION_DELIMITER.join([str(PBKDF2_ITERATIONS), _hexlify(salt), _hexlify(pbkdf2)])


if __name__ == '__main__':
    print(create_hash("wisdomG4rd3n"))
