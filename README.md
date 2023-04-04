# segway.mdseg

Code library that brings up the back-end of MD-Seg.

## Installation
```
pip install -U git+https://github.com/htem/segway.mdseg#egg=segway.mdseg
```

### Neuroglancer

You'll also need to separately install a front-end based on a modified `neuroglancer`:
`pip install git+https://github.com/htem/neuroglancer_mdseg@segway_pr_v2#egg=neuroglancer`

If that gives you an error, you'll need to debug why the build failed. To debug, clone and build the repo manually:
```
git clone https://github.com/htem/neuroglancer_mdseg
cd neuroglancer_mdseg
git checkout segway_pr_v2
pip install .
```
