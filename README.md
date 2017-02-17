# Instructions

1. clone the repo: https://github.com/periscope-ps/UNISrt
2. `pip install virtualenv` or pip3 if you have it
3. `virtualenv -p /your/python/version venv`  where `venv` can be any name
   for example `virtualenv -p /usr/local/bin/python3 venv`
4. `source venv/bin/activate`
5. `python -V` to confirm that you're using the same python as specified in step 3.
6. `python setup.py build install test` should do a bunch of things

# Usage

```python
from unis import Runtime

rt = Runtime("http://unis.crest.iu.edu:8888")
rt.metadata  # you can `dir(rt)` this to see more

# example
for md in rt.metadata:
    print md.name  # can read it...
    # md.name = random()  # you CAN clobber the existing UNIS database very easily (so please don't)
```
