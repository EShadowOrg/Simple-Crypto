import websockets as ws
import asyncio
import threading
import time

connection = None
connect_lock = threading.Lock()
stop = asyncio.Event()

async def test_ws():
    url = "wss://echo.websocket.org"
    while not stop.is_set():
        try:
            print("Connecting to WebSocket...")
            async with ws.connect(url) as websocket:
                print("WebSocket connected")
                with connect_lock:
                    global connection
                    connection = websocket
                stop_task = asyncio.create_task(stop.wait())
                while not stop.is_set():
                    msg_task = asyncio.create_task(websocket.recv())
                    done, pending = await asyncio.wait(
                        [msg_task, stop_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if stop.is_set():
                        msg_task.cancel()
                        print("Closing WebSocket...")
                        await connection.close()
                        print("WebSocket closed")
                        break
                    for task in done:
                        msg = task.result()
                    print(f"Received: {msg}")
        except ws.exceptions.ConnectionClosedOK:
            print("WebSocket closed normally.")
            break
        except ws.ConnectionClosedError as e:
            print(f"WebSocket closed with error: {e}. Reconnecting...")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"WebSocket error: {e}. Reconnecting...")
            await asyncio.sleep(2)

def start_test_ws():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(test_ws())

def send_message(message):
    with connect_lock:
        global connection
        if connection is not None:
            asyncio.run(connection.send(message))
        else:
            print("No active connection.")

if __name__ == "__main__":
    ws_thread = threading.Thread(target=start_test_ws, daemon=True)
    ws_thread.start()
    time.sleep(5)  # Wait for connection to establish

    send_message("Hello, WebSocket!")
    time.sleep(2)

    send_message("Goodbye, WebSocket!")
    time.sleep(2)

    print("Stopping WebSocket...")
    stop.set()
    ws_thread.join(timeout=20)

    if not ws_thread.is_alive():
        print("WebSocket thread terminated.")