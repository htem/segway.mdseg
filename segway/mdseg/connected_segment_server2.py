import os

import numpy as np
from cachetools import cached, RRCache

from funlib.geometry import Roi, Coordinate
from funlib.math import cantor_number, inv_cantor_number

def check_blocksize_consistency(big_bs, small_bs):
    assert len(big_bs) == len(small_bs)
    for a, b in zip(big_bs, small_bs):
        assert a % b == 0

class ConnectedSegmentServer():

    def __init__(
            self,
            blockstore,
            hierarchy_lut_path,
            super_lut_pre,
            find_segment_block_size,
            super_block_size,
            fragments_block_size,
            merge_function='hist_quant_50',
            voxel_size=(40, 4, 4),
            super_offset_hack=(0, 0, 0),
            base_threshold=0.5,
            cantor_number_offset=0,
            debug_voxel_size=None,
            ):

        self.super_lut_pre = os.path.join(hierarchy_lut_path, super_lut_pre)
        voxel_size = Coordinate(voxel_size)
        if debug_voxel_size is None:
            debug_voxel_size = voxel_size
        debug_voxel_size = Coordinate(debug_voxel_size)

        self.blockstore = blockstore

        check_blocksize_consistency(find_segment_block_size, fragments_block_size)
        check_blocksize_consistency(super_block_size, fragments_block_size)

        fragments_block_size = Coordinate(fragments_block_size)
        find_segment_block_size = Coordinate(find_segment_block_size)
        super_block_size = Coordinate(super_block_size)

        super_offset_hack = Coordinate(super_offset_hack)
        check_blocksize_consistency(super_offset_hack, fragments_block_size)
        super_offset_frag_nblock = super_offset_hack // fragments_block_size

        size_of_voxel = Roi((0,)*3, voxel_size).size
        fragments_block_roi = Roi((0,)*3, fragments_block_size)
        self.num_voxels_in_fragment_block = fragments_block_roi.size//size_of_voxel

        self.super_offset_frag_nblock = super_offset_frag_nblock
        self.local_chunk_size = find_segment_block_size // fragments_block_size
        self.super_chunk_size = super_block_size // find_segment_block_size
        self.fragments_block_size = fragments_block_size
        self.fragments_block_size_pix = fragments_block_size / voxel_size
        self.fragments_block_size_pix_model = fragments_block_size / debug_voxel_size
        self.voxel_size = voxel_size
        self.find_segment_block_size = find_segment_block_size
        self.super_block_size = super_block_size
        self.base_threshold = base_threshold
        self.cantor_number_offset = cantor_number_offset
        self.debug_voxel_size = debug_voxel_size


    def id2index(self, block_id):
        '''Calculating an n-dim index number from a linear index'''
        # self.cantor_number_offset = 2
        # print(f'self.cantor_number_offset: {self.cantor_number_offset}')
        # return inv_cantor_number(block_id + self.cantor_number_offset)
        return inv_cantor_number(block_id - self.cantor_number_offset)

    def index2id(self, block_index):
        '''Calculating a linear index number from an n-dim index'''
        block_id = int(cantor_number(block_index))
        return block_id + self.cantor_number_offset

    def get_super_index(self, fragment_id):
        super_id = int(fragment_id)
        print("super_id:", super_id)
        block_id = int(super_id / self.num_voxels_in_fragment_block)
        print("block_id:", block_id)
        fragment_index = Coordinate(self.id2index(block_id))
        print("fragment_index:", fragment_index)
        fragment_index -= self.super_offset_frag_nblock
        print("adjusted fragment_index:", fragment_index)
        local_index = fragment_index // self.local_chunk_size
        print("self.local_chunk_size:", self.local_chunk_size)
        print("local_index:", local_index)
        super_index = local_index // self.super_chunk_size
        print("self.super_chunk_size:", self.super_chunk_size)
        print("super_index:", super_index)
        return super_index

    def get_roi_fragments(self, fragment_id):
        chunk_id = int(fragment_id / self.num_voxels_in_fragment_block)
        chunk_index = Coordinate(self.id2index(chunk_id))
        print(f'chunk_index: {chunk_index}')
        roi_begin = chunk_index*self.fragments_block_size
        return Roi(roi_begin, self.fragments_block_size)

    @cached(cache=RRCache(maxsize=128*1024*1024))
    def get_super_cc(
            self,
            fragment_id,
            threshold,
            ):

        # super_index = self.get_super_index(fragment_id)
        roi = self.get_roi_fragments(fragment_id)
        print(f'fragment roi: {roi}')
        print(f'fragment roi (pix): {roi/self.debug_voxel_size}')
        lut = self.blockstore[threshold][roi.begin]
        adj_fragments = []
        for edge in lut:
            if edge[0] == fragment_id:
                adj_fragments.append(edge[1])
            if edge[1] == fragment_id:
                adj_fragments.append(edge[0])
        return adj_fragments

    def find_connected_super_fragments(
            self,
            selected_super_fragments,
            no_grow_super_fragments,
            threshold,
            z_only=False,
            ):

        connected_components = []
        processed_components = set()
        print(no_grow_super_fragments)
        no_grow_super_fragments = [int(n) for n in no_grow_super_fragments]
        no_grow_super_fragments = set(no_grow_super_fragments)
        selected_super_fragments = [int(n) for n in selected_super_fragments]

        for selected_super in selected_super_fragments:

            if selected_super == 0:
                continue
            if selected_super in processed_components:
                continue

            processed_components.add(selected_super)

            connected_components.append(selected_super)

            if selected_super in no_grow_super_fragments:
                continue

            cc = self.get_super_cc(selected_super, threshold=self.base_threshold)

            if z_only:
                selected_super_index = self.get_super_index(selected_super)
                selected_super_index = (selected_super_index[1], selected_super_index[2])
                filtered = []
                for component in cc:
                    index = self.get_super_index(component)
                    index = (index[1], index[2])
                    if index == selected_super_index:
                        filtered.append(component)
                cc = filtered

            cc = set(cc) - no_grow_super_fragments

            connected_components.extend(cc)

        connected_components = set(connected_components)

        # print("connected_components:", connected_components)
        if threshold != self.base_threshold:
            base_components = []
            for component in connected_components:
                # print("component:", component)
                tmp = set(self.get_base_subsegments(component, threshold))
                if tmp.isdisjoint(no_grow_super_fragments):
                    base_components.extend(tmp)
            connected_components |= set(base_components)

        # return list(set(connected_components))
        return list(connected_components)

