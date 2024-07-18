from datetime import datetime, timezone

def convert_time_to_rfc3339(datetime):
    """
    convert time string to RFC3339 format
    """
    return datetime.astimezone().isoformat()

    return 
    # current_time = datetime.now(timezone.utc)
    # current_time.isoformat() # '2021-07-13T15:28:51.818095+00:00'
    # date_obj = datetime.strptime(date_str, "%y-%m-%d")
    # return datetime.fromisoformat(date_str).astimezone().isoformat()