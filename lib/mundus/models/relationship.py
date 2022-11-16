from dataclasses import dataclass

@dataclass(frozen=True)
class Relationship(object):
    colRef: str
    subject: "Entity"
    target: "Entity"
    index: str = None

    def items(self):
        result = {"target": self.target.selfRef, "subject": self.subject.selfRef}
        if self.index is not None:
            result["index"] = self.index
        return result.items()
    def __iter__(self):
        return iter(self.items())

@dataclass
class RelationshipList(object):
    container: "Container"
    href: str
    owner: "Entity"

    def append(self, other: "Entity", idx=None):
        href = self.href.split('/')
        if href[3] == "subject":
            rel = Relationship(href[2], self.owner, other, idx)
        else:
            rel = Relationship(href[2], other, self.owner, idx)
        self.container._add_relationship(rel)

    def __iter__(self):
        if not hasattr(self, "_cache"):
            self._cache = self.container.find_relationship(self.href)
        return iter(self._cache)

    def __len__(self):
        if not hasattr(self, "_cache"):
            self._cache = self.container.find_relationship(self.href)        
        return len(self._cache)
