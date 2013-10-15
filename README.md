# Riaktor
Riak data access using [Reactor](https://github.com/reactor/reactor/).

---

### Building and installing

To use this early, experimental code, you'll need to install the Maven artifacts locally:

    git clone git://github.com/jbrisbin/riaktor.git
    cd riaktor
    ./gradlew install

Then use the artifacts in your build:

    dependencies {
      compile 'com.jbrisbin.riaktor:riaktor:0.1.0.ALPHA1'
    }

The tests assume you have a Riak instance running locally.

---

Riaktor is Apache 2.0 licensed.