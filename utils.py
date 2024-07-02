import psycopg2


PG_HOST = "172.25.102.198"
# PG_PREPROD_HOST = "172.25.104.25"
PG_PREPROD_HOST = "172.25.104.31"
PG_RO_PORT = "5001"
PG_PORT = "5000"
PG_USER = "postgres"
PG_PASSWORD = "qBnvO90j0NEsF3ucdQiM"
# PG_PREPROD_PASSWORD = "YfYIBhl0MtZ6"
PG_PREPROD_PASSWORD = "pi2Rvogua8x3L4dK4f9Y"

def slack_code_block_to_text(code_block):
    """
    Convert a slack code block to text.
    """
    return code_block.replace('```', '').replace('`', '').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').strip()


def slack_validate_sql_request(slack_app, event, ts):
    channel_id = event.get('item', {}).get('channel')
    parent_message = slack_app.client.conversations_history(
        channel=channel_id,
        latest=ts,
        limit=1,
        inclusive=True,
    )
    parent_message = parent_message.get('messages', [])[0]
    message_ts = parent_message.get('ts')
    parent_message_user = parent_message.get('user')
    # Check if the message is edited
    if parent_message.get('edited') is not None:
        print(f"Message is edited, ignoring.")
        return None, None, None
    # If message has `:white_check_mark:` or `:x:` or `:60fps_parrot:` emoji reaction, then ignore the message.
    reactions = parent_message.get('reactions', [])
    message_ts = parent_message.get('ts')
    for reaction in reactions:
        if reaction.get('name') in ["white_check_mark", "x", "60fps_parrot"]:
            print(f"Message already has a `:white_check_mark:` or `:x:` or `:60fps_parrot:` emoji reaction, ignoring.")
            return None, None, None
    # Remove the `:eyes:` emoji reaction from the message and add a `:60fps_parrot:` emoji reaction to the message
    slack_app.client.reactions_remove(
        channel=channel_id,
        name="eyes",
        timestamp=message_ts
    )
    try:
        slack_app.client.reactions_add(
            channel=channel_id,
            name="60fps_parrot",
            timestamp=message_ts
        )
    except Exception as e:
        print(f"Failed to add reaction: {e}")
    return message_ts, channel_id, parent_message

def run_return_sql(slack_app, channel_id, message_ts, database_name, query, read_only=True, prod=False):
    try:
        conn = psycopg2.connect(
            host=PG_HOST if prod else PG_PREPROD_HOST,
            port=PG_RO_PORT if read_only else PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD if prod else PG_PREPROD_PASSWORD,
            dbname=database_name,
        )
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Failed with error: {e}")
        # Post in a thread that the query failed
        slack_app.client.chat_postMessage(
            channel=channel_id,
            text=f"Query failed {'on preprod' if not prod else ''}: {e}",
            thread_ts=message_ts
        )
        # Remove the `:60fps_parrot:` emoji reaction from the message and add a `:x:` emoji reaction to the message
        slack_app.client.reactions_remove(
            channel=channel_id,
            name="60fps_parrot",
            timestamp=message_ts
        )
        try:
            slack_app.client.reactions_add(
                channel=channel_id,
                name="x",
                timestamp=message_ts
            )
        except Exception as e:
            print(f"Failed to add reaction: {e}")
        return
    if read_only:
        column_names = [desc[0] for desc in cur.description]
        results = [column_names] + cur.fetchall()
        if len(results) == 1:
            print(f"No results found")
            return "No results found"
        return results
    else:
        raw_result = cur.statusmessage
        print(f"Query executed successfully: {raw_result}")
        return raw_result
    
    
def slack_thread_query_success(slack_app, channel_id, message_ts):
    # Post in a thread that the query was successful
    slack_app.client.chat_postMessage(
        channel=channel_id,
        text=f"DONE!",
        thread_ts=message_ts
    )
    # Remove the `:60fps_parrot:` emoji reaction from the message and add a `:white_check_mark:` emoji reaction to the message
    slack_app.client.reactions_remove(
        channel=channel_id,
        name="60fps_parrot",
        timestamp=message_ts
    )
    try:
        slack_app.client.reactions_add(
            channel=channel_id,
            name="white_check_mark",
            timestamp=message_ts
        )
    except Exception as e:
        print(f"Failed to add reaction: {e}")