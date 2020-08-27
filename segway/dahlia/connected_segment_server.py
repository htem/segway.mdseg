# import sys
import os

import numpy as np
from cachetools import cached, RRCache

import daisy


def check_blocksize_consistency(big_bs, small_bs):
    assert len(big_bs) == len(small_bs)
    for a, b in zip(big_bs, small_bs):
        assert a % b == 0


class ConnectedSegmentServer():

    def __init__(
            self,
            hierarchy_lut_path,
            super_lut_pre,
            find_segment_block_size,
            super_block_size,
            fragments_block_size,
            merge_function='hist_quant_50',
            voxel_size=(40, 4, 4),
            super_offset_hack=(0, 0, 0),
            base_threshold=0.5,
            ):

        self.super_lut_pre = os.path.join(hierarchy_lut_path, super_lut_pre)

        check_blocksize_consistency(find_segment_block_size, fragments_block_size)
        check_blocksize_consistency(super_block_size, fragments_block_size)

        fragments_block_size = daisy.Coordinate(fragments_block_size)
        find_segment_block_size = daisy.Coordinate(find_segment_block_size)
        super_block_size = daisy.Coordinate(super_block_size)

        super_offset_hack = daisy.Coordinate(super_offset_hack)
        check_blocksize_consistency(super_offset_hack, fragments_block_size)
        super_offset_frag_nblock = super_offset_hack // fragments_block_size

        size_of_voxel = daisy.Roi((0,)*3, voxel_size).size()
        fragments_block_roi = daisy.Roi((0,)*3, fragments_block_size)
        self.num_voxels_in_fragment_block = fragments_block_roi.size()//size_of_voxel

        self.super_offset_frag_nblock = super_offset_frag_nblock
        self.local_chunk_size = find_segment_block_size // fragments_block_size
        self.super_chunk_size = super_block_size // find_segment_block_size
        self.fragments_block_size = fragments_block_size
        self.voxel_size = voxel_size
        self.find_segment_block_size = find_segment_block_size
        self.super_block_size = super_block_size
        self.base_threshold = base_threshold

    def get_super_index(self, fragment_id):
        super_id = int(fragment_id)
        # print("super_id:", super_id)
        block_id = int(super_id / self.num_voxels_in_fragment_block)
        # print("block_id:", block_id)
        fragment_index = daisy.Coordinate(daisy.Block.id2index(block_id))
        # print("fragment_index:", fragment_index)
        fragment_index -= self.super_offset_frag_nblock
        # print("adjusted fragment_index:", fragment_index)
        local_index = fragment_index // self.local_chunk_size
        # print("self.local_chunk_size:", self.local_chunk_size)
        # print("local_index:", local_index)
        super_index = local_index // self.super_chunk_size
        # print("self.super_chunk_size:", self.super_chunk_size)
        # print("super_index:", super_index)
        return super_index

    @cached(cache=RRCache(maxsize=128*1024*1024))
    def get_base_subsegments(
            self,
            super_fragment_id,
            threshold,
            ):
        super_index = self.get_super_index(super_fragment_id)
        super_block_id = daisy.Block.index2id(super_index)

        super_lut_dir = self.super_lut_pre + '_%d' % int(self.base_threshold*100)
        lut_file = os.path.join(
            super_lut_dir,
            'threshold_map_%d' % int(threshold*100),
            str(super_block_id) + '.npz')
        # print("Loading", lut_file)
        connected_components = np.load(lut_file, allow_pickle=True)['threshold_map']

        for cc in connected_components:
            if super_fragment_id in cc:
                # print(cc)
                return cc

        # assert False
        return []

    @cached(cache=RRCache(maxsize=128*1024*1024))
    def get_super_cc(
            self,
            super_fragment_id,
            threshold,
            ):

        super_index = self.get_super_index(super_fragment_id)
        super_block_id = daisy.Block.index2id(super_index)
        # print("super_block_id:", super_block_id)

        super_lut_dir = self.super_lut_pre + '_%d' % int(threshold*100)
        lut_file = os.path.join(super_lut_dir, 'edges_super2super', str(super_block_id) + '.npz')
        # print("Loading", lut_file)
        lut = np.load(lut_file)['edges']

        # print(lut)

        cc = []
        for edge in lut:
            if edge[0] == super_fragment_id:
                cc.append(edge[1])
            if edge[1] == super_fragment_id:
                cc.append(edge[0])

        # lut_file = os.path.join(super_lut_dir, 'nodes_super', str(super_block_id) + '.npz')
        # # print("Loading", lut_file)
        # lut = np.load(lut_file)['nodes']
        # print("nodes_super:", lut)

        # lut_file = os.path.join(super_lut_dir, 'seg_local2super', str(super_block_id) + '.npz')
        # # print("Loading", lut_file)
        # lut = np.load(lut_file)['seg']
        # print("seg_local2super:", lut)

        # lut_file = os.path.join(super_lut_dir, 'edges_super2local', str(super_block_id) + '.npz')
        # # print("Loading", lut_file)
        # lut = np.load(lut_file)['edges']
        # print("edges_super2local:", lut)

        return cc

    def find_connected_super_fragments(
            self,
            selected_super_fragments,
            no_grow_super_fragments,

            threshold,
            z_only=False,
            ):

        connected_components = []
        processed_components = set()
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


if __name__ == "__main__":
    '''Test cases'''

    hierarchy_lut_path = '/n/groups/htem/temcagt/datasets/cb2/segmentation/tri/cb2_segmentation/outputs/cb2/201906_purkinje_cell/output.zarr/luts/fragment_segment'
    super_lut_pre = 'super_2x4x4_hist_quant_50'

    prserver = ConnectedSegmentServer(
        hierarchy_lut_path=hierarchy_lut_path,
        super_lut_pre=super_lut_pre,

        find_segment_block_size=(4000, 4096, 4096),
        super_block_size=(8000, 16384, 16384),
        fragments_block_size=(80, 2048, 2048),

        super_offset_hack=(4000, 6144, 10240)
        )

    print(prserver.find_connected_super_fragments(
            selected_super_fragments=[4775281688885],
            no_grow_super_fragments=[],
            threshold=.4,
            ))
