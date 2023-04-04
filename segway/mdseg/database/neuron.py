
class Neuron():

    def __init__(
            self,
            name,
            segments=[],
            blacklist_segments=[],
            annotator=None,
            cell_type=None,
            cell_subtype='',
            tags=[],
            location_tags=[],
            location_tags_zyx=[],
            # notes=[],
            notes='',
            location_notes=[],
            location_notes_zyx=[],
            finished=False,
            reviewed=False,
            soma_loc={'z': 0, 'y': 0, 'x': 0},
            parent_segment='',
            # children=[],
            prevNeuron=None,
            uncertain_xyz='',
            mergers_xyz='',
            ):

        self.segments = segments
        self.children_segments = []
        self.segments_by_children = {}
        self.blacklist_segments = blacklist_segments
        self.cell_type = cell_type
        self.cell_subtype = cell_subtype
        self.tags = tags
        self.location_tags = location_tags
        self.location_tags_zyx = location_tags_zyx
        self.notes = notes
        self.location_notes = location_notes
        self.location_notes_zyx = location_notes_zyx
        self.uncertain_xyz = uncertain_xyz
        self.mergers_xyz = mergers_xyz
        self.reviewed = reviewed
        self.finished = finished
        self.soma_loc = soma_loc
        self.parent_segment = parent_segment
        self.children = []  # db-calculated value, not storable to DB

        self.version = 0
        if prevNeuron is not None:
            # inherit attrs from prevNeuron
            for attr, value in prevNeuron.__dict__.items():
                setattr(self, attr, value)
            self.version = prevNeuron.version + 1

        # these attrs should not be inherited from prevNeuron
        self.name = name
        self.name_prefix = ''

        if '.' in self.name:
            # this "neuron" is a child segment
            self.name_prefix = self.name.rsplit('.')[0]
        elif '_' in self.name:
            # a regular "parent" neuron
            self.name_prefix = self.name.rsplit('_')[0]

        self.annotator = annotator
        self.prevNeuron = prevNeuron

        self.finalized = False
        self.checked_subset = False
        self.checked_mapping = False

    def check_subset(self, no_subset_check=False):

        if no_subset_check:
            self.checked_subset = True
            return True

        self.checked_subset = True
        if self.prevNeuron and not no_subset_check:
            prev_segs = set(self.prevNeuron.segments)
            prev_segs -= set(self.children_segments)
            if not set(self.segments).issuperset(prev_segs):
                self.checked_subset = False

        return self.checked_subset

    def finalize(
            self,
            blacklist_segments=None,
            ):
        assert self.name != ''
        assert len(self.segments)

        self.segments = set(self.segments)

        assert self.checked_subset

        if blacklist_segments:
            self.blacklist_segments = blacklist_segments

        assert self.annotator in PREAPPROVED_ANNOTATORS, f"Annotator ({self.annotator}) not in PREAPPROVED_ANNOTATORS"
        assert self.cell_type in PREDEFINED_CELL_TYPES

        # TODO: check for tags consistency
        # check tag loc

        assert len(self.soma_loc) == 3
        assert 'z' in self.soma_loc
        assert 'y' in self.soma_loc
        assert 'x' in self.soma_loc

        if self.parent_segment == '' and '.' in self.name:
            self.parent_segment = self.name.rsplit('.', maxsplit=1)[0]

        if len(self.uncertain_xyz) and 'uncertain_continuation' not in self.tags:
            self.tags.append('uncertain_continuation')

        if len(self.mergers_xyz) and 'merge_errors' not in self.tags:
            self.tags.append('merge_errors')

        self.finalized = True

    def is_finalized(self):
        return self.finalized

    def is_substantially_modified(self):
        return set(self.segments) != set(self.prevNeuron.segments)

    def get_backup_name(self):
        return "%s.%d" % (self.name, self.version)

    def to_json(self):

        # need to convert numbers to string for json
        segments = [str(k) for k in self.segments]
        blacklist_segments = [str(k) for k in self.blacklist_segments]

        return {
            'neuron_name': self.name,
            'name_prefix': self.name_prefix,
            'segments': segments,
            'blacklist_segments': blacklist_segments,
            'cell_type': self.cell_type,
            'cell_subtype': self.cell_subtype,
            'tags': self.tags,
            'location_tags': [],
            'location_tags_zyx': [],
            'notes': self.notes,
            'uncertain_xyz': self.uncertain_xyz,
            'mergers_xyz': self.mergers_xyz,
            'location_notes': [],
            'location_notes_zyx': [],
            'version': self.version,
            'annotator': self.annotator,
            'reviewed': self.reviewed,
            'finished': self.finished,
            'soma_loc': self.soma_loc,
            'parent_segment': self.parent_segment,
        }

    @staticmethod
    def from_dict(d):
        n = Neuron(d['neuron_name'])
        if 'segments' in d:
            n.segments = [int(k) for k in d['segments']]
        if 'blacklist_segments' in d:
            n.blacklist_segments = [int(k) for k in d['blacklist_segments']]
        if 'annotator' in d:
            n.annotator = d['annotator']
        if 'cell_type' in d:
            n.cell_type = d['cell_type']
        if 'cell_subtype' in d:
            n.cell_subtype = d['cell_subtype']
        if 'tags' in d:
            n.tags = d['tags']
        if 'notes' in d:
            n.notes = d['notes']
        if 'version' in d:
            n.version = d['version']
        if 'annotator' in d:
            n.annotator = d['annotator']
        if 'reviewed' in d:
            n.reviewed = d['reviewed']
        if 'finished' in d:
            n.finished = d['finished']
        if 'soma_loc' in d:
            n.soma_loc = d['soma_loc']
            for k, v in n.soma_loc.items():
                n.soma_loc[k] = int(v)
        if 'parent_segment' in d:
            n.parent_segment = d['parent_segment']
        if 'uncertain_xyz' in d:
            n.uncertain_xyz = d['uncertain_xyz']
        if 'mergers_xyz' in d:
            n.mergers_xyz = d['mergers_xyz']
        return n
