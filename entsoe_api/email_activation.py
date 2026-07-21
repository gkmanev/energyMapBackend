"""Email-verification helpers for the API's built-in Django users."""
from __future__ import annotations

from urllib.parse import quote

import requests
from django.conf import settings
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_encode


class ActivationTokenGenerator(PasswordResetTokenGenerator):
    """Invalidate an activation token as soon as its account is activated."""

    def _make_hash_value(self, user, timestamp):
        return f"{user.pk}{user.password}{user.is_active}{timestamp}{user.email}"


activation_token_generator = ActivationTokenGenerator()


def send_activation_email(user) -> None:
    """Send a one-time activation link through Resend's REST API."""
    api_key = settings.RESEND_API_KEY
    sender = settings.RESEND_FROM_EMAIL
    if not api_key or not sender:
        raise RuntimeError("RESEND_API_KEY and RESEND_FROM_EMAIL must be configured.")

    uid = urlsafe_base64_encode(force_bytes(user.pk))
    token = activation_token_generator.make_token(user)
    activation_url = (
        f"{settings.API_PUBLIC_URL.rstrip('/')}/api/auth/activate/{uid}/{quote(token, safe='')}/"
    )
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "from": sender,
            "to": [user.email],
            "subject": "Activate your visualize.energy account",
            "html": (
                "<p>Welcome to visualize.energy.</p>"
                f'<p><a href="{activation_url}">Activate your account</a></p>'
                "<p>This link expires in three days.</p>"
            ),
        },
        timeout=10,
    )
    response.raise_for_status()
