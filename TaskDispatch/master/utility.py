# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 17:48
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com
from functools import wraps


def create_new_sequence_node(zk_client, base_path, prefix, is_ephemeral=False):
    if not zk_client.exists(base_path):
        zk_client.ensure_path(base_path)

    new_node = zk_client.create( base_path+'/'+prefix, ''.encode('utf-8'), sequence=True, ephemeral=is_ephemeral )
    return new_node

class SetGetMixin():
    def get(path_variable):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if not self.zk_client.exists(getattr(self, path_variable)):
                    return None
                return func(self, *args, **kwargs)

            return wrapper
        return decorator

    def set(path_variable):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                self.zk_client.ensure_path(getattr(self, path_variable))
                return func(self, *args, **kwargs)
            return wrapper
        return decorator