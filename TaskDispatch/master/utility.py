# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 17:48
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

def create_new_sequence_node(zk_client, base_path, prefix, is_ephemeral=False):
    if not zk_client.exists(base_path):
        zk_client.ensure_path(base_path)

    new_node = zk_client.create( base_path+'/'+prefix, ''.encode('utf-8'), sequence=True, ephemeral=is_ephemeral )
    return new_node
