import asyncio
import aiohttp
import sys

from hailtop.config import get_deploy_config
from hailtop.auth import service_auth_headers
from hailtop.tls import get_context_specific_ssl_client_session


def init_parser(parser):
    subparsers = parser.add_subparsers()
    set_parser = subparsers.add_parser(
        'set',
        help='Set a Hail query resource value.',
        description='Set a Hail query resource value.')
    unset_parser = subparsers.add_parser(
        'unset',
        help='Unset a Hail query resource value (restore to default behavior).',
        description='Unset a Hail query resource value (restore to default behavior).')
    get_parser = subparsers.add_parser(
        'get',
        help='Get the value of a Hail query resource (or all values of a specific resource type).',
        description='Get the value of a Hail query resource.')

    resource_types = ['flag']

    set_parser.set_defaults(action='set')
    set_parser.add_argument("resource", type=str, choices=resource_types,
                            help="Resource type.")
    set_parser.add_argument("name", type=str,
                            help="Name of resource.")
    set_parser.add_argument("value", type=str,
                            help="Value to set.")

    unset_parser.set_defaults(action='unset')
    unset_parser.add_argument("resource", type=str, choices=resource_types,
                              help="Resource type.")
    unset_parser.add_argument("name", type=str,
                              help="Name of resource.")

    get_parser.set_defaults(action='get')
    get_parser.add_argument("resource", type=str, choices=resource_types,
                            help="Resource type.")
    get_parser.add_argument("name", type=str, nargs='*',
                            help="Name(s) of resource.")


class QueryClient:
    def __init__(self):
        self._deploy_config = get_deploy_config()
        self._session = None

    async def __aenter__(self):
        headers = service_auth_headers(self._deploy_config, 'query')
        self._session = get_context_specific_ssl_client_session(
            timeout=aiohttp.ClientTimeout(total=60), headers=headers)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def get_request(self, path):
        async with self._session.get(
                self._deploy_config.url('query', f'/api/v1alpha/{path}')) as resp:
            if resp.status >= 400:
                print(f'HTTP Response code was {resp.status}')
                print(await resp.text())
                sys.exit(1)
            return await resp.json()

    async def set_flag(self, name, value):
        old = await self.get_request(f'flags/set/{name}/{value}')
        print(f'Set {name} to {value}. Old value: {old}')

    async def unset_flag(self, name):
        old = await self.get_request(f'flags/unset/{name}')
        print(f'Unset {name}. Old value: {old}')

    async def get_flag(self, names):
        if len(names) == 0:
            all = await self.get_request('flags/get')
            l = max(len(k) for k in all.keys())
            for k, v in all.items():
                print(f'{k.rjust(l)}: ' + (f'"{v}"' if v is not None else 'null'))
        else:
            l = max(len(k) for k in names)
            for name in names:
                v = await self.get_request(f'flags/get/{name}')
                print(f'{name.rjust(l)}: ' + (f'"{v}"' if v is not None else 'null'))


async def submit(args):
    async with QueryClient() as client:
        if args.action == 'set' and args.resource == 'flag':
            await client.set_flag(args.name, args.value)
        elif args.action == 'unset' and args.resource == 'flag':
            await client.unset_flag(args.name)
        elif args.action == 'get' and args.resource == 'flag':
            await client.get_flag(args.name)


def main(args):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(submit(args))
    loop.run_until_complete(loop.shutdown_asyncgens())
