import argparse
import asyncio

import aiohttp


async def consume(args: argparse.Namespace) -> None:
    params = {"id": args.producer_id}
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(args.server_url, params=params) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    print(f"Received frame ({len(msg.data)} bytes)")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WebSocket consumer receiving frames.")
    parser.add_argument(
        "--server-url", default="ws://localhost:9000/ws/consumer", help="Hub consumer endpoint."
    )
    parser.add_argument("--producer-id", required=True, help="Producer ID to subscribe to.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(consume(args))


if __name__ == "__main__":
    main()
