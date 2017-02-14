
class DictDiffer(object):

    def __init__(self, old, new):
        self.old = old
        self.new = new
        self.old_set = set(self.old.keys())
        self.new_set = set(self.new.keys())
        self.intersect = self.new_set.intersection(self.old_set)

    def added(self):
        return self.new_set - self.intersect

    def removed(self):
        return self.old_set - self.intersect

    def changed(self):
        return set(k for k in self.intersect if self.old[k] != self.new[k])
