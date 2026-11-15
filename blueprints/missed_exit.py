"""Missed-exit warnings for the broker tab that is open.

The list is this broker's warnings only. Confirm sends the close that warning
is holding. Clear puts the warning away and does not send that close.
"""

from flask import Blueprint, jsonify, session

from services.downtime_resume import clear_warning, confirm_warning, warnings_for_tab
from utils.logging import get_logger
from utils.session import check_session_validity

logger = get_logger(__name__)

missed_exit_bp = Blueprint("missed_exit_bp", __name__)


def _broker_selected() -> bool:
    return bool(session.get("broker"))


@missed_exit_bp.route("/auth/missed-exits", methods=["GET"])
def list_missed_exits():
    """Warnings for the broker named by this tab."""
    if not session.get("logged_in") or not _broker_selected():
        return jsonify({"status": "success", "warnings": []})
    try:
        warnings = warnings_for_tab()
    except Exception:
        logger.exception("Could not list missed-exit warnings")
        return jsonify({"status": "error", "warnings": []}), 500
    return jsonify({"status": "success", "warnings": warnings})


@missed_exit_bp.route("/auth/missed-exits/<int:warning_id>/confirm", methods=["POST"])
@check_session_validity
def confirm_missed_exit(warning_id: int):
    """Send the close this warning is holding."""
    if not _broker_selected():
        return jsonify({"status": "error", "message": "This tab has no broker."}), 400
    result = confirm_warning(warning_id)
    if not result.get("sent"):
        return jsonify(
            {
                "status": "error",
                "message": result.get("error") or "That warning has no close to send.",
                "sent": False,
            }
        ), 409
    return jsonify({"status": "success", "sent": True, "message": "The held close was sent."})


@missed_exit_bp.route("/auth/missed-exits/<int:warning_id>/clear", methods=["POST"])
@check_session_validity
def clear_missed_exit(warning_id: int):
    """Remove the warning. This does not send its close."""
    if not _broker_selected():
        return jsonify({"status": "error", "message": "This tab has no broker."}), 400
    if not clear_warning(warning_id):
        return jsonify({"status": "error", "message": "That warning is already resolved."}), 404
    return jsonify(
        {
            "status": "success",
            "sent": False,
            "message": "Cleared. The held close was not sent.",
        }
    )
