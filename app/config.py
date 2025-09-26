import os
import secrets

LOCAL_GOVERNMENT = set([
    "Akoko North-East".lower(),
    "Akoko North-West".lower(),
    "Akoko South-East".lower(),
    "Akoko South-West".lower(),
    "Akure North".lower(),
    "Akure South".lower(),
    "Emure-Ile".lower(),
    "Idanre".lower(),
    "Ifedore".lower(),
    "Igbara-oke".lower(),
    "Ilaje".lower(),
    "Ile Oluji".lower(),
    "Irele".lower(),
    "Isua Akoko".lower(),
    "Odigbo".lower(),
    "Oka Akoko".lower(),
    "Okitipupa".lower(),
    "Ondo East".lower(),
    "Ondo West".lower(),
    "Ose".lower(),
    "Owo".lower()])

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY') or secrets.token_hex(30)
    ADMIN_PAGE_PAGINATION = 15