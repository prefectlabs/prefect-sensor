"""A tiny in-process SFTP server for end-to-end sensor tests."""

from __future__ import annotations

import errno
import os
import socket
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Optional

import paramiko


class _PasswordServer(paramiko.ServerInterface):
    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def check_auth_password(self, username: str, password: str) -> int:
        if username == self._username and password == self._password:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_channel_request(self, kind: str, chanid: int) -> int:
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class _FilesystemSFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, server: paramiko.ServerInterface, root: str) -> None:
        super().__init__(server)
        self._root = Path(root).resolve(strict=False)

    def _resolve(self, path: str) -> Path:
        remote = PurePosixPath(path)
        parts = remote.parts[1:] if remote.is_absolute() else remote.parts
        resolved = self._root.joinpath(*parts).resolve(strict=False)
        if resolved != self._root and self._root not in resolved.parents:
            raise OSError(errno.EACCES, os.strerror(errno.EACCES))
        return resolved

    def list_folder(self, path: str):
        try:
            directory = self._resolve(path)
            entries = []
            for child in sorted(directory.iterdir(), key=lambda item: item.name):
                attr = paramiko.SFTPAttributes.from_stat(child.stat())
                attr.filename = child.name
                entries.append(attr)
            return entries
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def stat(self, path: str):
        try:
            return paramiko.SFTPAttributes.from_stat(self._resolve(path).stat())
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def lstat(self, path: str):
        return self.stat(path)


class SFTPTestServer:
    """A disposable SSH/SFTP server backed by a temporary directory."""

    def __init__(self, root: Path, username: str, password: str) -> None:
        self.root = root
        self.username = username
        self.password = password
        self.host = "127.0.0.1"
        self.port = 0
        self._socket: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._host_key = paramiko.RSAKey.generate(2048)
        self._transports: list[paramiko.Transport] = []

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("SFTPTestServer is already running.")
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, 0))
        self._socket.listen(5)
        self.port = self._socket.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        self._ready.wait(timeout=5)

    def stop(self) -> None:
        self._stop.set()
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        for transport in self._transports:
            transport.close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _serve(self) -> None:
        self._ready.set()
        assert self._socket is not None
        while not self._stop.is_set():
            try:
                client, _ = self._socket.accept()
            except OSError:
                break
            transport = paramiko.Transport(client)
            transport.add_server_key(self._host_key)
            transport.set_subsystem_handler(
                "sftp",
                paramiko.SFTPServer,
                _FilesystemSFTPServer,
                root=str(self.root),
            )
            server = _PasswordServer(self.username, self.password)
            self._transports.append(transport)
            try:
                transport.start_server(server=server)
                channel = transport.accept(10)
                if channel is None:
                    transport.close()
                    continue
                while transport.is_active() and not self._stop.is_set():
                    time.sleep(0.05)
            finally:
                transport.close()
                if transport in self._transports:
                    self._transports.remove(transport)
