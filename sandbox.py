import asyncio
import websockets

import ingest.streaming.polygon

asyncio.get_event_loop().run_until_complete(ingest.streaming.polygon.run())


