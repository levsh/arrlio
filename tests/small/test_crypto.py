import os

from arrlio import crypto


def test_generate_private_key():
    assert crypto.generate_private_key()


def test_generate_crypto_key():
    crypto.generate_crypto_key(length=64)


def test_symmetric():
    # short key
    key = os.urandom(8)

    data = os.urandom(365)
    assert crypto.s_decrypt(crypto.s_encrypt(data, key), key) == data

    # data = {"a": "a" * 1000}
    # assert crypto.s_decrypt_json_base64(crypto.s_encrypt_json_base64(data, key), key) == data

    # long key
    key = os.urandom(32)

    data = os.urandom(365)
    assert crypto.s_decrypt(crypto.s_encrypt(data, key), key) == data

    # data = {"a": "a" * 1000}
    # assert crypto.s_decrypt_json_base64(crypto.s_encrypt_json_base64(data, key), key) == data


def test_asymmetric():
    pri_key = crypto.generate_private_key()
    pub_key = pri_key.public_key()

    data = os.urandom(346)
    assert crypto.a_decrypt(crypto.a_encrypt(data, pub_key), pri_key) == data

    # data = {"a": "a" * 1000}
    # assert crypto.a_decrypt_json_base64(crypto.a_encrypt_json_base64(data, pub_key), pri_key) == data
