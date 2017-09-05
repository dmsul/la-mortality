from clean.pr2.rawio import raw_int_qcer
from clean.pr2.firmgroups import get_grouprep, group_lists_full


class FirmIDXwalk(object):
    # Load data into object so it's only read from disk once.

    def __init__(self):
        # `raw_int_qcer` should be the full list of `facid`s that have emissions
        # data before *any* cleaning is done
        self.facids = tuple(sorted(raw_int_qcer()['facid'].unique().tolist()))
        self.group_reps = group_lists_full().apply(get_grouprep)

    def get_firmid(self, facid, group_rep=False):
        """Get `firm_id` for a `facid`, or `inverse` of that."""
        if group_rep:
            query = self.group_reps.loc[facid]
        else:
            query = facid

        return self.facids.index(query)

    def get_facid(self, firm_id):
        return self.facids[firm_id]
