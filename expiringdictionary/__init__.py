import asyncio
from typing import (
    Any,
    Dict,
    List,
    Union,
    Optional
)
import datetime

class ExpiringDictionary:
    def __init__(self) -> None:
        self.dict: Dict[str, Any] = {}
        self.rl: Dict[str, int] = {}
        self.delete: Dict[str, Dict[str, Union[int, int]]] = {}

    async def do_expiration(
        self, 
        key: str, 
        expiration: int
    ) -> None:
        await asyncio.sleep(expiration)
        self.dict.pop(key, None)

    async def set(
        self, 
        key: str, 
        value: Any, 
        expiration: int = 60
    ) -> int:
        self.dict[key] = value
        asyncio.ensure_future(self.do_expiration(key, expiration))
        return 1

    async def delete(self, key: str) -> bool:
        deleted_count = self.dict.pop(key, 0)
        return bool(deleted_count)

    async def get(self, key: str) -> Any:
        return self.dict.get(key, 0)

    async def keys(self) -> List[Any]:
        return list(self.dict.keys())

    async def do_delete(self, key: str) -> None:
        self.dict.pop(key, None)
        self.delete[key] = {'last': int(datetime.datetime.now().timestamp())}

    def is_ratelimited(self, key: str) -> bool:
        return self.dict.get(key, 0) >= self.rl.get(key, 0)

    def time_remaining(self, key: str) -> int:
        last = self.delete.get(key, {}).get('last', 0)
        remaining = (last + self.delete.get(key, {}).get('bucket', 60)) - int(datetime.datetime.now().timestamp())
        return max(remaining, 0) if key in self.dict and self.dict[key] >= self.rl.get(key, 0) else 0

    async def ratelimit(
        self, 
        key: str, 
        amount: int, 
        bucket: int = 60
    ) -> bool:
        self.dict.setdefault(key, 0)
        self.rl.setdefault(key, amount)
        self.delete.setdefault(key, {'bucket': bucket, 'last': 0})

        last, now = self.delete[key]['last'], int(
            datetime.datetime.now().timestamp())
        if last + bucket <= now:
            self.dict.pop(key, None)
            self.delete[key]['last'], self.dict[key] = now, 0

        self.dict[key] = self.dict.get(key, 0) + 1
        return self.dict[key] >= self.rl.get(key, 0)
