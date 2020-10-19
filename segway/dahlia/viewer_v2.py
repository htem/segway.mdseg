# import sys
# import os
import copy
import datetime
import socket

import neuroglancer

from .db_server import PREDEFINED_TAGS
from .db_server import PREDEFINED_CELL_TYPES
from .db_server import PREAPPROVED_ANNOTATORS
from .db_server import PREDEFINED_CELL_SEGMENT_TYPES

default_color_order = [
    'matlab1',
    'matlab2',
    'matlab3',
    'matlab4',
    'matlab5',
    'matlab6',
    'matlab7',
    'oldmatlab1',
    'oldmatlab2',
    'oldmatlab3',
    'oldmatlab4',
    'oldmatlab5',
    'oldmatlab6',
    'oldmatlab7',
    'white',
    'grey',
    'darkgrey',
    'lightgrey',
    # 'black',
    'red',
    'darkred',
    'lightred',
    'orange',
    'darkorange',
    'lightorange',
    'orange_',
    'yellow',
    'darkyellow',
    'lightyellow',
    'green',
    'darkgreen',
    'lightgreen',
    'cyan',
    'darkcyan',
    'lightcyan',
    ]

color_map = {
    'matlab1'    : '#0072bd',
    'matlab2'    : '#d95319',
    'matlab3'    : '#edb120',
    'matlab4'    : '#7e2f8e',
    'matlab5'    : '#77ac30',
    'matlab6'    : '#4dbeee',
    'matlab7'    : '#a2142f',

    'oldmatlab1' : '#0000ff',
    'oldmatlab2' : '#008000',
    'oldmatlab3' : '#ff0000',
    'oldmatlab4' : '#00bfbf',
    'oldmatlab5' : '#bf00bf',
    'oldmatlab6' : '#bfbf00',
    'oldmatlab7' : '#404040',

    'white': '#ffffff',
    'grey': '#808080',
    'darkgrey': '#404040',
    'lightgrey': '#c0c0c0',
    'black': '#000000',
    'red': '#ff0000',
    'darkred': '#800000',
    'lightred': '#ff8080',
    'orange': '#ff8000',
    'darkorange': '#804000',
    'lightorange': '#ffc080',
    'orange_': '#ed4617',
    'yellow': '#ffff00',
    'darkyellow': '#808000',
    'lightyellow': '#ffff80',
    'green': '#00ff00',
    'darkgreen': '#008000',
    'lightgreen': '#80ff80',
    'cyan': '#00ffff',
    'darkcyan': '#008080',
    'lightcyan': '#80ffff',
    'blue': '#0000ff',
    'darkblue': '#000080',
    'lightblue': '#8080ff',
    'violet': '#8000ff',
    'darkviolet': '#400080',
    'lightviolet': '#c080ff',
    'purple': '#ff00ff',
    'darkpurple': '#800080',
    'lightpurple': '#ff80ff',
    'magenta': '#ff0080',
    'darkmagenta': '#800040',
    'lightmagenta': '#ff80c0',
}


def new_viewer():
    for i in range(33400, 33500):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(('0.0.0.0', i))
            break
        except OSError as err:
            if err.errno == 98:
                # Address already in use
                continue
            else:
                raise err
        finally:
            probe.close()

    print(i)
    neuroglancer.set_server_bind_address('0.0.0.0', i)
    viewer = neuroglancer.Viewer()
    return viewer


class GrowTracker():

    def __init__(self):
        self.clear()

    def clear(self):
        self.grow_tracker_selections0 = None
        self.grow_tracker_selections1 = None

    def update(self, selections, grow_count):
        if self.grow_tracker_selections0 is None or grow_count:
            self.grow_tracker_selections1 = self.grow_tracker_selections0
            self.grow_tracker_selections0 = set(selections)

    def diff(self, selections):
        if self.grow_tracker_selections1 is None:
            return set()
        return selections - self.grow_tracker_selections1


class DahliaViewer():

    def __init__(
            self,
            proof_reading_server,
            neuron_db,
            viewer=None,
            ):

        if viewer is None:
            viewer = new_viewer()

        self.neuron_db = neuron_db
        self.prserver = proof_reading_server
        self.viewer = viewer

        viewer.actions.clear()

        viewer.actions.add('grow-segments', self.grow_segments)
        viewer.actions.add('grow-segments-super', lambda s: self.grow_segments(s, super_threshold=True))
        viewer.actions.add('grow-segments-diff', lambda s: self.grow_segments(s, super_threshold=True, diff_grow=True))
        viewer.actions.add('grow-segments-z', lambda s: self.grow_segments(s, z_only=True))
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['keyq'] = 'grow-segments'
            s.input_event_bindings.viewer['shift+keyq'] = 'grow-segments-super'
            s.input_event_bindings.viewer['keyw'] = 'grow-segments-diff'
            s.input_event_bindings.viewer['control+keyq'] = 'grow-segments-z'

        self.clipboard_sel = []
        viewer.actions.add('copy-selections', lambda s: self.copy_selections(s))
        viewer.actions.add('cut-selections', lambda s: self.paste_selections(s, cut=True))
        viewer.actions.add('paste-selections', lambda s: self.paste_selections(s, cut=False))
        viewer.actions.add('join-selections', self.join_selections)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+shift+keyc'] = 'copy-selections'
            s.input_event_bindings.viewer['control+keyx'] = 'cut-selections'
            s.input_event_bindings.viewer['control+shift+keyv'] = 'paste-selections'
            s.input_event_bindings.viewer['control+keyj'] = 'join-selections'

        viewer.actions.add('undo-selections', self.undo_selections)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+keyz'] = 'undo-selections'
        viewer.actions.add('redo-selections', self.redo_selections)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+keyy'] = 'redo-selections'

        self.blacklist_segments = []
        viewer.actions.add('add-blacklist', self.add_blacklist)
        viewer.actions.add('remove-blacklist', self.remove_blacklist)
        viewer.actions.add('clear-blacklist', self.clear_blacklist)
        viewer.actions.add('show-active-blacklist', self.show_active_blacklist)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+keyb'] = 'add-blacklist'
            s.input_event_bindings.viewer['control+shift+keyb'] = 'remove-blacklist'
            s.input_event_bindings.viewer['control+alt+shift+keyb'] = 'show-active-blacklist'
            s.input_event_bindings.viewer['control+alt+shift+keyn'] = 'clear-blacklist'

        self.neuron_states = {}
        # empty_neuron_state = {}
        empty_neuron = neuron_db.create_neuron('', no_check=True)
        empty_neuron_state = self._get_state_from_neuron(empty_neuron)
        self.neuron_states[''] = copy.deepcopy(empty_neuron_state)
        self.tab_neuron_name = ['' for n in range(10)]
        self.empty_neuron_state = empty_neuron_state

        self.saved_selection_states = [[] for n in range(10)]
        self.current_tab = 0
        # unrolled because I couldn't get lambdas to work right in a loop :(
        viewer.actions.add('load-selections-0', lambda s: self.load_selections(s, 0))
        viewer.actions.add('load-selections-1', lambda s: self.load_selections(s, 1))
        viewer.actions.add('load-selections-2', lambda s: self.load_selections(s, 2))
        viewer.actions.add('load-selections-3', lambda s: self.load_selections(s, 3))
        viewer.actions.add('load-selections-4', lambda s: self.load_selections(s, 4))
        viewer.actions.add('load-selections-5', lambda s: self.load_selections(s, 5))
        viewer.actions.add('load-selections-6', lambda s: self.load_selections(s, 6))
        viewer.actions.add('load-selections-7', lambda s: self.load_selections(s, 7))
        viewer.actions.add('load-selections-8', lambda s: self.load_selections(s, 8))
        viewer.actions.add('load-selections-9', lambda s: self.load_selections(s, 9))
        viewer.actions.add('save-selections-0', lambda s: self.save_selections(s, 0))
        viewer.actions.add('save-selections-1', lambda s: self.save_selections(s, 1))
        viewer.actions.add('save-selections-2', lambda s: self.save_selections(s, 2))
        viewer.actions.add('save-selections-3', lambda s: self.save_selections(s, 3))
        viewer.actions.add('save-selections-4', lambda s: self.save_selections(s, 4))
        viewer.actions.add('save-selections-5', lambda s: self.save_selections(s, 5))
        viewer.actions.add('save-selections-6', lambda s: self.save_selections(s, 6))
        viewer.actions.add('save-selections-7', lambda s: self.save_selections(s, 7))
        viewer.actions.add('save-selections-8', lambda s: self.save_selections(s, 8))
        viewer.actions.add('save-selections-9', lambda s: self.save_selections(s, 9))
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+shift+digit0'] = 'save-selections-0'
            s.input_event_bindings.viewer['control+shift+digit1'] = 'save-selections-1'
            s.input_event_bindings.viewer['control+shift+digit2'] = 'save-selections-2'
            s.input_event_bindings.viewer['control+shift+digit3'] = 'save-selections-3'
            s.input_event_bindings.viewer['control+shift+digit4'] = 'save-selections-4'
            s.input_event_bindings.viewer['control+shift+digit5'] = 'save-selections-5'
            s.input_event_bindings.viewer['control+shift+digit6'] = 'save-selections-6'
            s.input_event_bindings.viewer['control+shift+digit7'] = 'save-selections-7'
            s.input_event_bindings.viewer['control+shift+digit8'] = 'save-selections-8'
            s.input_event_bindings.viewer['control+shift+digit9'] = 'save-selections-9'
            s.input_event_bindings.viewer['shift+digit0'] = 'load-selections-0'
            s.input_event_bindings.viewer['shift+digit1'] = 'load-selections-1'
            s.input_event_bindings.viewer['shift+digit2'] = 'load-selections-2'
            s.input_event_bindings.viewer['shift+digit3'] = 'load-selections-3'
            s.input_event_bindings.viewer['shift+digit4'] = 'load-selections-4'
            s.input_event_bindings.viewer['shift+digit5'] = 'load-selections-5'
            s.input_event_bindings.viewer['shift+digit6'] = 'load-selections-6'
            s.input_event_bindings.viewer['shift+digit7'] = 'load-selections-7'
            s.input_event_bindings.viewer['shift+digit8'] = 'load-selections-8'
            s.input_event_bindings.viewer['shift+digit9'] = 'load-selections-9'

        self.selection_history = []
        self.selection_history_idx = 0
        self.selection_history_max = 1000
        for i in range(self.selection_history_max):
            self.selection_history.append([])
        self.grow_tracker = GrowTracker()

        # neuron saving/loading to DB
        viewer.actions.add('save-neuron', self.save_neuron)
        viewer.actions.add('load-neuron', self.load_neuron_by_name)
        viewer.actions.add('load-neuron-by-segment', self.load_neuron_by_segment)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+shift+keys'] = 'save-neuron'
            s.input_event_bindings.viewer['control+keyo'] = 'load-neuron'
            s.input_event_bindings.viewer['control+shift+keyo'] = 'load-neuron-by-segment'

        # Search functionality
        viewer.actions.add('search-neuron', self.search_neuron)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+keyf'] = 'search-neuron'

        # color tabs
        viewer.actions.add('set-color', self.set_color)
        viewer.actions.add('clear-color', self.clear_color)
        with viewer.config_state.txn() as s:
            s.input_event_bindings.viewer['control+keyl'] = 'set-color'

        # UI button function mapping
        viewer.actions.add('prSomaLocCopyLocEvent', self.prSomaLocCopyLocEvent)
        viewer.actions.add(
                'dbLoadNeuronNameButtonEvent',
                lambda s: self.load_neuron_by_name(s, 0))
        viewer.actions.add(
                'dbLoadNeuronNameButton1Event',
                lambda s: self.load_neuron_by_name(s, 1))
        viewer.actions.add(
                'dbLoadNeuronNameButton2Event',
                lambda s: self.load_neuron_by_name(s, 2))
        viewer.actions.add(
                'dbLoadNeuronNameButton3Event',
                lambda s: self.load_neuron_by_name(s, 3))
        viewer.actions.add(
                'clNeuronColorButton', self.clNeuronColorButton)

    def save_internal_states(self, state):
        state['blacklist_segments'] = copy.deepcopy(self.blacklist_segments)
        state['neuron_states'] = copy.deepcopy(self.neuron_states)
        state['tab_neuron_name'] = copy.deepcopy(self.tab_neuron_name)
        state['saved_selection_states'] = copy.deepcopy(self.saved_selection_states)
        state['current_tab'] = copy.deepcopy(self.current_tab)
        state['clipboard_sel'] = copy.deepcopy(self.clipboard_sel)
        state['selection_history'] = copy.deepcopy(self.selection_history)
        state['selection_history_idx'] = copy.deepcopy(self.selection_history_idx)

    def _set_wrapped_property(self, key, val):
        new_state = copy.deepcopy(self.viewer.state)
        new_state.layers['seg'].layer._json_data[key] = val
        self.viewer.set_state(new_state)

    def _set_non_wrapped_property(self, key0, key1, val):
        new_state = copy.deepcopy(self.viewer.state)
        new_state.layers['seg'].layer._json_data[key0][key1] = val
        self.viewer.set_state(new_state)

    # def _get_wrapped_property(self, key):
    #     if key in self.viewer.state.layers['seg'].layer:
    #         return self.viewer.state.layers['seg'].layer[key]
    #     else:
    #         return None

    def _get_non_wrapped_property(self, key0, key1=None, default=None):
        if key1 is None:
            if key0 in self.viewer.state.layers['seg'].layer._json_data:
                ret = self.viewer.state.layers['seg'].layer._json_data[key0]
            else:
                ret = None
        else:
            if key0 not in self.viewer.state.layers['seg'].layer._json_data:
                ret = None
            else:
                if key1 not in self.viewer.state.layers['seg'].layer._json_data[key0]:
                    ret = None
                else:
                    ret = self.viewer.state.layers['seg'].layer._json_data[key0][key1]

        if default is not None and (ret == '' or ret is None):
            return default

        return ret

    def __also_load_children(self):
        with_children = True
        if self._get_non_wrapped_property('neurondb', 'dbLoadWithoutChildren', False):
            with_children = False
        return with_children


    # @staticmethod
    # def _cast_to_true_false(val):
    #     if val == '1' or val == 'true':
    #         return True
    #     else:
    #         return False

    @staticmethod
    def _get_viewer_segments(state):
        if 'layers' in state._json_data:
            layer = state._json_data['layers'][2]
        else:
            layer = state._json_data['viewerState']['layers'][2]
        if 'segments' not in layer:
            return []
        return [int(n) for n in layer['segments']]

    def restore_internal_states(self, state):

        if 'blacklist_segments' in state:
            self.blacklist_segments = copy.deepcopy(state['blacklist_segments'])
        if 'neuron_states' in state:
            self.neuron_states = copy.deepcopy(state['neuron_states'])
        if 'tab_neuron_name' in state:
            self.tab_neuron_name = copy.deepcopy(state['tab_neuron_name'])
        if 'saved_selection_states' in state:
            self.saved_selection_states = copy.deepcopy(state['saved_selection_states'])
        if 'current_tab' in state:
            self.current_tab = copy.deepcopy(state['current_tab'])
        if 'clipboard_sel' in state:
            self.clipboard_sel = copy.deepcopy(state['clipboard_sel'])
        if 'selection_history' in state:
            self.selection_history = copy.deepcopy(state['selection_history'])
        if 'selection_history_idx' in state:
            self.selection_history_idx = copy.deepcopy(state['selection_history_idx'])

    def _set_selections(self, selections):
        new_state = copy.deepcopy(self.viewer.state)
        new_state.layers['seg'].segments = selections
        # print("Setting selections to", selections)
        self.viewer.set_state(new_state)

    def _save_undo_history(self, selections=None, no_inc=False):
        '''Saves `selections` to current (empty) slot and increases to next empty slot
        '''
        if selections is None:
            selections = self.viewer.state.layers['seg'].segments

        self.selection_history[self.selection_history_idx] = selections

        if not no_inc:
            self.selection_history_idx = (self.selection_history_idx + 1) % self.selection_history_max

    def set_selections(self, selections):
        self._save_undo_history()
        self._set_selections(selections)

    def copy_selections(self, s):
        selected = DahliaViewer._get_viewer_segments(s)
        self._save_undo_history(selected, no_inc=True) # TODO
        self.clipboard_sel = selected
        self.status_print("Copied %d segments"%len(selected))

    def paste_selections(self, s, cut=False):
        selected = list(DahliaViewer._get_viewer_segments(s))
        self._save_undo_history(selected)
        if not cut:
            selected.extend(self.clipboard_sel)
        else:
            selected = set(selected)
            selected.difference_update(set(self.clipboard_sel))
        self._set_selections(selected)
        self.status_print("Pasted selections")

    def undo_selections(self, s):
        # user might have manually selected more segments here
        # we'd need to save them first before restoring
        self._save_undo_history(no_inc=True)

        # now actual undo
        if self.selection_history_idx == 0:
            prev_idx = self.selection_history_max - 1
        else:
            prev_idx = self.selection_history_idx - 1
        selections = self.selection_history[prev_idx]
        self._set_selections(selections)
        self.selection_history_idx = prev_idx
        self.status_print("Undo to slot %d"%prev_idx)

    def redo_selections(self, s):
        self._save_undo_history(no_inc=True)
        self.selection_history_idx = (self.selection_history_idx + 1) % self.selection_history_max
        selections = self.selection_history[self.selection_history_idx]
        self._set_selections(selections)
        self.status_print("Redo to slot %d"%self.selection_history_idx)

    def add_blacklist(self, s):
        seg_id = s.selected_values['seg']
        self.blacklist_segments.append(seg_id)
        self.status_print("Blacklist %d"%seg_id)

    def remove_blacklist(self, s):
        seg_id = s.selected_values['seg']
        if seg_id in self.blacklist_segments:
            self.blacklist_segments.remove(seg_id)
            self.status_print("Un-blacklist %d"%seg_id)
        else:
            self.status_print("%d was not in blacklist"%seg_id)

    def clear_blacklist(self, s):
        self.blacklist_segments = []
        self.status_print("Clear blacklist!")

    def show_active_blacklist(self, s):
        current_segs = self.viewer.state.layers['seg'].segments
        active = list(set(current_segs).intersection(set(self.blacklist_segments)))
        self.set_selections(active)
        self.status_print("Show %d active blacklisted segments"%len(active))

    # def _grow_segments(self, s):
    #     selected = DahliaViewer._get_viewer_segments(s)
    #     cc = self.prserver.find_connected_super_fragments(
    #         selected_super_fragments=selected,
    #         no_grow_super_fragments=[],
    #         threshold=.5,
    #         z_only=False,
    #         )
    #     return cc

    def __filter_blacklist_seg(self, blacklist, segments):
        # selected = DahliaViewer._get_viewer_segments(s)
        filtered_blacklist = []
        segments = set(segments)
        for b in set(blacklist):
            if b in segments:
                filtered_blacklist.append(b)
            else:
                cc = self.prserver.find_connected_super_fragments(
                    selected_super_fragments=[b],
                    no_grow_super_fragments=[],
                    threshold=.5,
                    z_only=False,
                    )

                if len(set(cc) & segments):
                    filtered_blacklist.append(b)
        return filtered_blacklist

    def grow_segments(self, s, z_only=False, super_threshold=False, diff_grow=False):

        selected = DahliaViewer._get_viewer_segments(s)
        threshold1 = float(self._get_non_wrapped_property(
            'pr', 'prGrowThreshold', default=0.5))
        threshold2 = float(self._get_non_wrapped_property(
            'pr', 'prSuperGrowThreshold', default=0.5))

        if not diff_grow:
            cc = self.prserver.find_connected_super_fragments(
                selected_super_fragments=selected,
                no_grow_super_fragments=self.blacklist_segments,
                threshold=threshold1,
                z_only=z_only,
                )

        grow_count = len(cc) - len(selected)

        self.grow_tracker.update(selected, grow_count)

        if grow_count > 0:
            self.set_selections(cc)
            self.status_print("Grown segments by %d" % grow_count) 
            return

        selected_set = set(selected)
        diff = self.grow_tracker.diff(selected_set)
        cc_tmp = []
        if len(diff):
            print("Diff grow threshold:", threshold2)
            cc_tmp = self.prserver.find_connected_super_fragments(
                selected_super_fragments=list(diff),
                no_grow_super_fragments=self.blacklist_segments,
                threshold=threshold2,
                z_only=z_only,
                )
        cc = list(selected_set | set(cc_tmp))

        grow_count = len(cc) - len(selected)
        if grow_count > 0:
            self.set_selections(cc)

        self.status_print("Grown segments by %d" % grow_count)

    def join_selections(self, s):
        # for each selected segment, find a single neuron object
        # previously saved. If there are multiple, error out.
        # then add the selected segments to the object

        selected_segs = DahliaViewer._get_viewer_segments(s)
        annotator = self._get_ui_annotator()
        if annotator not in PREAPPROVED_ANNOTATORS:
            self.status_print("SAVE FAILED: annotator %s not in preapproved list %s" % (annotator, PREAPPROVED_ANNOTATORS))
            return False


        # names = set([None])
        name_list = []
        for seg in selected_segs:
            name = self.neuron_db.find_neuron_with_segment_id(seg)
            if name:
                name_list.append(name)
            # names.add(name)
        # names.remove(None)
        names = set(name_list)

        if len(names) == 0:
            self.status_print("ERROR: None of the selections are saved as neurons")
            return
        elif len(names) > 1:
            self.status_print(f"ERROR: Selections saved as multiple objects {names}")
            return

        if len(name_list) == len(selected_segs):
            self.status_print(f"ERROR: all segments already joined to {name_list[0]}")
            return


        neuron_name = name_list[0]
        neuron = self.neuron_db.get_neuron(neuron_name, with_children=False)

        # make various checks
        ret = neuron.check_subset()
        if not ret:
            self.status_print("SAVE FAILED %s: neuron not superset of previously saved version" % neuron_name)
            return

        neuron.annotator = annotator
        neuron.segments.extend(selected_segs)
        # neuron.finalize()
        try:
            neuron.finalize()
            # neuron.finalize(grow_once_segments=grow_once_segments)
        except Exception as e:
            print(e)
            self.status_print("SAVE FAILED %s: unknown exception during neuron.finalize()" % neuron_name)
            return
        try:
            ret, msg = self.neuron_db.check_neuron_mapping(neuron)
            if ret is not True:
                self.status_print("SAVE FAILED %s: check_neuron_mapping: %s" % (neuron_name, msg))
                return
        except Exception as e:
            print(e)
            self.status_print("SAVE FAILED %s: unknown exception during neuron_db.check_neuron_mapping()" % neuron_name)
            return

        # finally save
        try:
            self.neuron_db.save_neuron(neuron)
        except Exception as e:
            print(e)
            self.status_print("SAVE FAILED %s: unknown exception during neuron_db.save_neuron()" % neuron_name)
            return

        self.status_print(f"Sucessfully joined segments to ({neuron_name})")

    def save_selections(self, s, n):
        self._save_undo_history(no_inc=True)
        selected = DahliaViewer._get_viewer_segments(s)
        self.saved_selection_states[n] = selected
        self.tab_neuron_name[n] = self.tab_neuron_name[self.current_tab]
        self.status_print("Saved selections to slot %d" % n)

    def load_selections(self, s, n):

        self.set_selections(self.saved_selection_states[n])

        # need to save current neuron metadata before switching
        self._set_state_from_viewer_state(self.neuron_states[self.tab_neuron_name[self.current_tab]])

        # load new neuron metadata and push to viewer
        new_state = self.neuron_states[self.tab_neuron_name[n]]
        self._set_viewer_state_from_state(new_state)
        self.current_tab = n

        self.grow_tracker.clear()

        self.status_print("Loaded selections from slot %d"%n)

    def status_print(self, status):
        with self.viewer.config_state.txn() as st:
            st.status_messages['hello'] = "%s: %s" % (datetime.datetime.now(), status)

    def _new_neuron_state(self):
        neuron_state = {}
        neuron_state['name'] = ''
        neuron_state['segments'] = []
        neuron_state['annotator'] = ''
        neuron_state['cell_type'] = ''
        neuron_state['tags'] = []
        return neuron_state

    def _get_ui_annotator(self):
        layer_json = self.viewer.state.layers['seg'].layer.to_json()
        layer_json = layer_json['pr']
        return layer_json['prAnnotator']

    def _set_state_from_viewer_state(self, target_state):
        layer_json = self.viewer.state.layers['seg'].layer.to_json()
        target_state['segments'] = [int(k) for k in layer_json['segments']]
        layer_json = layer_json['pr']
        target_state['name'] = layer_json.get('prNeuronName', '')
        target_state['annotator'] = layer_json.get('prAnnotator', '')
        target_state['cell_type'] = layer_json.get('prCellType', '')
        target_state['tags'] = layer_json.get('prTags', '').split()
        # target_state['finished'] = True if (layer_json.get('prFinished') is True or layer_json.get('prFinished') == "1" or layer_json.get('prFinished') == "true") else False
        # target_state['reviewed'] = True if (layer_json.get('prReviewed') is True or layer_json.get('prReviewed') == "1" or layer_json.get('prReviewed') == "true") else False
        target_state['finished'] = layer_json.get('prFinished', False)
        target_state['reviewed'] = layer_json.get('prReviewed', False)
        target_state['notes'] = layer_json.get('prNotes', '')
        target_state['uncertain_xyz'] = layer_json.get('prUncertainCon', '')
        target_state['mergers_xyz'] = layer_json.get('prMergers', '')
        target_state['soma_loc'] = layer_json.get('prSomaLoc', '')

    def _set_neuron_from_state(self, neuron, state):
        '''Mainly for saving procedure'''

        neuron.name = state['name']
        neuron.segments = state['segments']
        # neuron.blacklist_segments = state['blacklist_segments']
        neuron.blacklist_segments = self.blacklist_segments
        neuron.annotator = state['annotator']
        neuron.cell_type = state['cell_type']
        neuron.tags = state['tags']
        neuron.notes = state['notes']
        neuron.uncertain_xyz = state['uncertain_xyz']
        neuron.mergers_xyz = state['mergers_xyz']
        neuron.finished = state['finished']
        neuron.reviewed = state['reviewed']
        # neuron.location_tags = state['location_tags']
        # print(state['soma_loc'])
        soma_xyz_loc = state['soma_loc']
        soma_xyz_loc = [int(k) for k in soma_xyz_loc.split(', ')]
        # if isinstance(state['soma_loc'], str):
        #     soma_xyz_loc = [n.strip() for n in soma_xyz_loc]
        # print('_set_neuron_from_state:soma_xyz_loc:', soma_xyz_loc)
        voxel_size_xyz = [16, 16, 40]
        soma_zyx_loc = {
            'z': int(soma_xyz_loc[2])*voxel_size_xyz[2],
            'y': int(soma_xyz_loc[1])*voxel_size_xyz[1],
            'x': int(soma_xyz_loc[0])*voxel_size_xyz[0],
        }
        # print('soma_zyx_loc:', soma_zyx_loc)
        neuron.soma_loc = soma_zyx_loc

    def _get_state_from_neuron(self, neuron):
        target_state = {}
        target_state['name'] = neuron.name
        target_state['segments'] = neuron.segments
        # target_state['blacklist_segments'] = [int(seg) for seg in neuron.blacklist_segments]
        target_state['annotator'] = neuron.annotator
        target_state['cell_type'] = neuron.cell_type
        target_state['tags'] = neuron.tags
        # target_state['finished'] = True if (neuron.finished == True or neuron.finished == "1" or neuron.finished == "true") else False
        # target_state['reviewed'] = True if (neuron.reviewed == True or neuron.reviewed == "1" or neuron.reviewed == "true") else False
        target_state['finished'] = neuron.finished
        target_state['reviewed'] = neuron.reviewed
        target_state['notes'] = neuron.notes
        print("neuron.uncertain_xyz:", neuron.uncertain_xyz)
        target_state['uncertain_xyz'] = neuron.uncertain_xyz
        target_state['mergers_xyz'] = neuron.mergers_xyz

        # print("_get_state_from_neuron:neuron.soma_loc:", neuron.soma_loc)

        soma_zyx_loc = neuron.soma_loc #  target_state['soma_loc']
        voxel_size_xyz = [16, 16, 40]
        soma_xyz_loc = [
                str(int(soma_zyx_loc['x']/voxel_size_xyz[0])),
                str(int(soma_zyx_loc['y']/voxel_size_xyz[1])),
                str(int(soma_zyx_loc['z']/voxel_size_xyz[2])),
        ]
        # print("soma_xyz_loc:", soma_xyz_loc)
        print("%s: %s" % (neuron.name, soma_xyz_loc))
        # print(soma_xyz_loc)
        target_state['soma_loc'] = ', '.join(soma_xyz_loc)
        # self.neuron_state['location_tags'] = neuron.location_tags
        return target_state

    def _print_xyz_loc(self, neuron):
        soma_zyx_loc = neuron.soma_loc
        voxel_size_xyz = [16, 16, 40]
        soma_xyz_loc = (
                (int(soma_zyx_loc['x']/voxel_size_xyz[0])),
                (int(soma_zyx_loc['y']/voxel_size_xyz[1])),
                (int(soma_zyx_loc['z']/voxel_size_xyz[2])),
        )
        print("%s: %s" % (neuron.name, soma_xyz_loc))

    def _set_viewer_state_from_state(self, from_state):
        new_state = copy.deepcopy(self.viewer.state)
        layer_json = new_state.layers['seg'].layer._json_data['pr']

        layer_json['prNeuronName'] = from_state['name']
        layer_json['prAnnotator'] = from_state['annotator']
        layer_json['prCellType'] = from_state['cell_type']
        layer_json['prTags'] = ' '.join(from_state['tags'])
        layer_json['prFinished'] = from_state['finished']
        layer_json['prReviewed'] = from_state['reviewed']
        layer_json['prNotes'] = from_state['notes']
        layer_json['prUncertainCon'] = from_state['uncertain_xyz']
        layer_json['prMergers'] = from_state['mergers_xyz']
        layer_json['prSomaLoc'] = from_state['soma_loc']
        # layer_json['loc_tags'] = []

        self.viewer.set_state(new_state)

    def _check_neuron_save_state_consistency(self, state):

        neuron_name = state['name']

        if neuron_name == '':
            self.status_print("SAVE FAILED: prNeuronName field is empty")
            return False

        if len(state['segments']) == 0:
            self.status_print("SAVE FAILED: no segments selected")
            return False

        if state['cell_type'] not in PREDEFINED_CELL_TYPES:
            self.status_print("SAVE FAILED %s: given cell type %s not in %s" % (neuron_name, state['cell_type'], PREDEFINED_CELL_TYPES))
            return False

        for tag in state['tags']:
            prepend_annotator = tag.split('_')[0]
            if tag not in PREDEFINED_TAGS and prepend_annotator not in PREAPPROVED_ANNOTATORS and tag not in PREDEFINED_CELL_TYPES:
                # if  in PREAPPROVED_ANNOTATORS:
                #     pass  # OK if tag is prepended with annotator_id
                # else:
                self.status_print("SAVE FAILED %s: tag %s not in %s nor in preapproved annotator list" % (neuron_name, tag, PREDEFINED_TAGS))
                return False

        if state['soma_loc'] == '':
            self.status_print("SAVE FAILED %s: soma_loc is empty" % (neuron_name))
            return False

        if state['annotator'] is None or state['annotator'] == '':
            self.status_print("SAVE FAILED %s: please sign the annotator field" % neuron_name)
            return False

        if state['annotator'] not in PREAPPROVED_ANNOTATORS:
            self.status_print("SAVE FAILED %s: annotator %s not in preapproved list %s" % (neuron_name, state['annotator'], PREAPPROVED_ANNOTATORS))
            return False

        return True

    def save_neuron(self, s):

        neuron_state = copy.deepcopy(self.empty_neuron_state)
        self._set_state_from_viewer_state(neuron_state)

        if not self._check_neuron_save_state_consistency(neuron_state):
            return

        neuron_name = neuron_state['name']
        print("Saving name:", neuron_name)

        neuron_type = neuron_state['cell_type']
        if neuron_type in PREDEFINED_CELL_SEGMENT_TYPES:
            if neuron_name[-1] != '_' and '.' not in neuron_name:
                # add generate basename format
                neuron_name = neuron_name + '.' + neuron_type + '_'

        if neuron_name[-1] == '_':
            # trailing `_` indicates basename, get a new one

            if neuron_type in PREDEFINED_CELL_SEGMENT_TYPES:
                if '.' not in neuron_name:
                    self.status_print("SAVE FAILED %s: segment type neuron (%s) needs to have subsegment naming" % neuron_type)
                    return

            neuron = self.neuron_db.create_neuron_with_prefix(neuron_name)
            print("New generated name:", neuron.name)
            neuron_name = neuron.name

        elif self.neuron_db.exists_neuron(neuron_name):
            neuron = self.neuron_db.get_neuron(neuron_name)

        else:
            neuron = self.neuron_db.create_neuron(neuron_name)

        self._set_neuron_from_state(neuron, neuron_state)
        neuron.name = neuron_name

        # override_set_check = DahliaViewer._cast_to_true_false(
        #     self._get_non_wrapped_property('pr', 'prOverrideSuperSetCheck'))
        # override_conflict_check = DahliaViewer._cast_to_true_false(
        #     self._get_non_wrapped_property('pr', 'prOverrideConflictCheck'))

        override_set_check = self._get_non_wrapped_property('pr', 'prOverrideSuperSetCheck')
        override_conflict_check = self._get_non_wrapped_property('pr', 'prOverrideConflictCheck')
        assert (override_set_check is True) or (override_set_check is False)
        assert (override_conflict_check is True) or (override_conflict_check is False)

        ret = neuron.check_subset(no_subset_check=override_set_check)
        if not ret:
            self.status_print("SAVE FAILED %s: neuron not superset of previously saved version" % neuron_name)
            return

        # try:
        #     grow_once_segments = self._grow_segments(s)
        # except Exception as e:
        #     # print(e)
        #     print("grow_once_segments failed")
        #     grow_once_segments = []
        # total_blacklist_segments = neuron.blacklist_segments
        # for s in total_blacklist_segments:
        filtered_blacklist = self.__filter_blacklist_seg(
            neuron.blacklist_segments, neuron.segments)

        print(f'filtered_blacklist: {filtered_blacklist}')

        try:
            neuron.finalize(blacklist_segments=filtered_blacklist)
            # neuron.finalize(grow_once_segments=grow_once_segments)
        except Exception as e:
            print(e)
            self.status_print("SAVE FAILED %s: unknown exception during neuron.finalize()" % neuron_name)
            return

        try:
            ret, msg = self.neuron_db.check_neuron_mapping(neuron, override_assignment=override_conflict_check)
            if ret is not True:
                self.status_print("SAVE FAILED %s: check_neuron_mapping: %s" % (neuron_name, msg))
                return
        except Exception as e:
            print(e)
            self.status_print("SAVE FAILED %s: unknown exception during neuron_db.check_neuron_mapping()" % neuron_name)
            return

        try:
            self.neuron_db.save_neuron(neuron)
        except Exception as e:
            print(e)
            self.status_print("SAVE FAILED %s: unknown exception during neuron_db.save_neuron()" % neuron_name)
            return

        # update neuron name if saved using prefix
        neuron_state['name'] = neuron.name
        self._set_non_wrapped_property('pr', 'prNeuronName', neuron.name)

        # save new state to slots
        self.neuron_states[neuron.name] = neuron_state
        self.tab_neuron_name[self.current_tab] = neuron.name

        self.status_print("Successfully saved selections as %s!" % neuron.name)

    def search_neuron(self, s):

        attrs = self.viewer.state.layers['seg'].layer.to_json()
        query = {}

        dbNeuronPrefix = attrs['neurondb'].get('dbNeuronPrefix', '')
        dbFindAnnotator = attrs['neurondb'].get('dbFindAnnotator', '')
        dbFindType = attrs['neurondb'].get('dbFindType', '')
        dbFindTags = attrs['neurondb'].get('dbFindTags', '')
        dbFindFinished = attrs['neurondb'].get('dbFindFinished', False)
        dbFindReviewed = attrs['neurondb'].get('dbFindReviewed', False)

        if dbNeuronPrefix != '':
            if dbNeuronPrefix[-1] == '_':
                self.status_print("ERROR: trailing `_` in prefix query")
                return
            query['name_prefix'] = dbNeuronPrefix

        if dbFindAnnotator != '':
            query['annotator'] = dbFindAnnotator

        if dbFindType != '':
            query['cell_type'] = dbFindType

        if dbFindTags != '':
            tags = dbFindTags.split()
            if len(tags) == 1:
                query['tags'] = tags[0]
            else:
                query['tags'] ={"$all": tags}

        # if dbFindFinished is True or dbFindFinished == '1' or dbFindFinished == "true":
        #     query['finished'] = True
        # if dbFindReviewed is True or dbFindReviewed == '1' or dbFindReviewed == "true":
        #     query['reviewed'] = True

        if dbFindFinished:
            query['finished'] = True
        if dbFindReviewed:
            query['reviewed'] = True

        print("search_neuron")
        print(query)
        if len(query) == 0:
            self.status_print("SEARCH FAILED: no query provided")
            return

        res = self.neuron_db.find_neuron_filtered(query)
        count = len(res)
        res = ', '.join(res)

        self._set_non_wrapped_property('neurondb', 'dbFindResult', res)
        self.status_print("Found %d neurons" % count)

    def load_neuron_by_name(self, s, slot=0):

        attrs = self.viewer.state.layers['seg'].layer.to_json()

        tab_name = attrs['tab']
        if tab_name != "Search DB":
            # not in correct tab for loading, ignore to protect the user
            return

        if slot == 0:
            name = attrs['neurondb'].get('dbLoadNeuronName', '')
        elif slot == 1:
            name = attrs['neurondb'].get('dbLoadNeuronName1', '')
        elif slot == 2:
            name = attrs['neurondb'].get('dbLoadNeuronName2', '')
        elif slot == 3:
            name = attrs['neurondb'].get('dbLoadNeuronName3', '')
        else:
            self.status_print("BUG: erroneous slot mapping %d" % slot)
            return

        if name == '':
            self.status_print("ERROR: empty load string")
            return

        return self._load_neuron(name, self.__also_load_children())

    def load_neuron_by_segment(self, s):

        selected = DahliaViewer._get_viewer_segments(s)
        if len(selected) == 0:
            self.status_print("ERROR: no segment selected")
            return
        elif len(selected) > 1:
            self.status_print("ERROR: more than one segment selected")
            return

        name = self.neuron_db.find_neuron_with_segment_id(selected[0])
        if name is None:
            self.status_print("ERROR: selected segment not saved as neuron")
            return

        if '.' in name:
            if self.__also_load_children():
                name = name.rsplit('.')[0]

        return self._load_neuron(name, with_children=True)

    def _load_neuron(self, name, with_children):

        if self.neuron_db.exists_neuron(name):
            neuron = self.neuron_db.get_neuron(name, with_children=with_children)
        elif self.neuron_db.exists_neuron(name, backup=True):
            neuron = self.neuron_db.get_neuron(name, backup=True, with_children=with_children)
        else:
            self.status_print("ERROR: %s not found" % name)
            return

        # print(neuron.name)
        # print(neuron.finished)

        # restore segments and blacklists
        blacklist = [int(seg) for seg in neuron.blacklist_segments]
        self.blacklist_segments.extend(blacklist)
        self.blacklist_segments = list(set(self.blacklist_segments))
        segs = [int(seg) for seg in neuron.segments]
        self.set_selections(segs)

        # restore other parameters
        neuron_state = self._get_state_from_neuron(neuron)
        annotator = self._get_non_wrapped_property('pr', 'prAnnotator')
        neuron_state['annotator'] = annotator
        self.neuron_states[neuron.name] = neuron_state
        self._set_viewer_state_from_state(neuron_state)

        # set tab attributes
        self.tab_neuron_name[self.current_tab] = neuron.name

        self.grow_tracker.clear()

        self.status_print("Loaded %s (v%d)" % (name, neuron.version))

    def set_color(self, s):

        request_color = self._get_non_wrapped_property('colors', 'set_color_val')

        if request_color is None or request_color == '':
            self.status_print("FAILED: set_color_val field is empty")
            return
        if request_color not in color_map and (
                request_color[0] != '#' or len(request_color) != 7):
            self.status_print("FAILED: set_color_val field should have the form #rrggbb")
            return


        selections = DahliaViewer._get_viewer_segments(s)
        if len(selections) == 0:
            self.status_print("FAILED: no segments selected")
            return

        current_color_map = self._get_non_wrapped_property('segmentColors')
        if current_color_map is None:
            current_color_map = {}

        for segment in selections:
            current_color_map[str(segment)] = request_color

        self._set_wrapped_property('segmentColors', dict(current_color_map))
        self.status_print("SUCCESS: set_color_val to %s" % request_color)

    def clNeuronColorButton(self, s):

        attrs = self.viewer.state.layers['seg'].layer.to_json()

        color_mapping = self._get_non_wrapped_property('colors', 'clNeuronColor')

        if color_mapping is None or len(color_mapping) == 0:
            self.status_print("FAILED: clNeuronColor field is empty")
            return

        also_load_neurons = self._get_non_wrapped_property('colors', 'clAlsoLoadNeurons')
        clear_before_load = self._get_non_wrapped_property('colors', 'clClearBeforeLoad')

        with_children = self.__also_load_children()

        neuron_loaded = False

        current_color_map = {}
        if not clear_before_load:
            current_color_map = self._get_non_wrapped_property('segmentColors')
            if current_color_map is None:
                current_color_map = {}

        color_mapping_split = []
        for l in color_mapping.split('\n'):
            for ll in l.split(', '):
                print(ll)
                color_mapping_split.append(ll)

        total_segs = []
        if also_load_neurons and not clear_before_load:
            total_segs = DahliaViewer._get_viewer_segments(s)

        default_color_index = 0

        for line in color_mapping_split:

            if len(line) == 0:
                continue

            line = line.split(': ')

            if len(line) == 1:
                idx = default_color_index % len(default_color_order)
                color = color_map[default_color_order[idx]]
                default_color_index += 1
                name = line[0]

            else:

                if len(line) != 2:
                    self.status_print("FAILED: clNeuronColor field is ill-formatted %s" % line)
                    return

                name = line[0]
                color = line[1]

                if color in color_map:
                    color = color_map[color]

                if color[0] != '#' or len(color) != 7:
                    self.status_print("FAILED: Color %s should have the form #rrggbb" % color)
                    return

            segs = []
            if name.split('.')[-1] == 'axon':
                # load all axon segments
                i = 0
                subsegment_name = '%s_%d' % (name, i)
                while self.neuron_db.exists_neuron(subsegment_name):
                    neuron = self.neuron_db.get_neuron(subsegment_name)
                    segs.extend([str(seg) for seg in neuron.segments])
                    i += 1
                    subsegment_name = '%s_%d' % (name, i)
                for seg in segs:
                    current_color_map[seg] = color

            elif self.neuron_db.exists_neuron(name):
                neuron = self.neuron_db.get_neuron(name, with_children=with_children)
                self._print_xyz_loc(neuron)
                segs.extend([str(seg) for seg in neuron.segments])
                for seg in segs:
                    current_color_map[seg] = color
                if not neuron_loaded:
                    self._load_neuron(name, with_children=with_children)
                    neuron_loaded = True

            else:
                try:
                    seg_id = int(name)
                    total_segs.append(seg_id)
                    current_color_map[seg_id] = color
                except:
                    # name is not an int number
                    pass

            total_segs.extend(segs)

        self._set_wrapped_property('segmentColors', dict(current_color_map))

        if also_load_neurons:
            self._set_selections(total_segs)

        self.status_print("SUCCESS: clNeuronColorButton")

    def clear_color(self, s):

        self._set_wrapped_property('segmentColors', dict())
        self.status_print("SUCCESS: cleared colors")

    def prSomaLocCopyLocEvent(self, s):

        coord = s._json_data['position']
        coord = [str(int(c)) for c in coord]
        coord = ', '.join(coord)
        self._set_non_wrapped_property('pr', 'prSomaLoc', coord)
        self.status_print("SUCCESS: Set soma loc to %s" % coord)
