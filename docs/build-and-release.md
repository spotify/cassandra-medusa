# How to release a new medusa version

Turns out there's a CI/CD pipeline in place thanks to the `build-info.yaml` file in the root of this repo.

Any change to the master branch will trigger a build using Spotify's managed build system [Tingle](https://developer.spotify.net/guides/04-tingle/index.html). This build will result in a debian package pushed to Spotify's debmirrors.

Medusa's puppet class ensures the _latest_ package gets installed on any host with `medusa` class.

The package has the following naming pattern:

```
# [version]-[date]-[commit]
0.0.2-2018-10-09-025475e
```

The version comes from `setup.py` in the root of this repo.
