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

**It is important to bump the version in `setup.py` diligently. Failing to do so leads to undesired behaviour.**

Two commits to master using the same version might result in something like this:

```
# from http://debmirror:9444/trusty/pool/stable/main/medusa/
spotify-medusa_0.0.2-2018-10-09-025475e_amd64.deb  09-Oct-2018 09:24             8481182
spotify-medusa_0.0.2-2018-10-09-0a669e1_amd64.deb  09-Oct-2018 13:12             8639368
spotify-medusa_0.0.2-2018-10-09-0bf9d23_amd64.deb  09-Oct-2018 11:46             8481736
```

There are three `0.0.2` versions from `2018-10-09`. They are sorted by their commit hashes. One thing is that lexicographical order of the hashes doesn't match their creation times.

Whats more problematic though is that this naming pattern confuses `apt`:

```
$ apt-cache policy spotify-medusa
spotify-medusa:
  Installed: 0.0.2-2018-10-09-025475e
  Candidate: 0.0.2-2018-10-09-025475e
  Version table:
 *** 0.0.2-2018-10-09-025475e 0
        600 http://debmirror:9444/trusty/ stable/main amd64 Packages
        100 /var/lib/dpkg/status
     0.0.2-2018-10-09-0bf9d23 0
        600 http://debmirror:9444/trusty/ stable/main amd64 Packages
     0.0.2-2018-10-09-0a669e1 0
        600 http://debmirror:9444/trusty/ stable/main amd64 Packages
```

When we ask puppet to `ensure => latest`, it will pick a wrong one.