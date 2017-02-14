from common.rfid import encode


def test_card_encode():
    encoded = encode('3321925635')
    assert encoded == b'039000C6'
