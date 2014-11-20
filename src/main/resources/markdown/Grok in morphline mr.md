* grok can be found dictionaries in Jars.
 * but `dictionaryResources` can be used for only files. No directory allowed.
 * `dictionaryFiles` can be set with directories. but actual local directory needed.

* `downloadHdfsFile` command is used for grok dictionaries in HDFS.
```
downloadHdfsFile {
  inputFiles : ["hdfs://c2202.mycompany.com/user/foo/configs/grok-patterns"]
}
grok {
  dictionaryFiles : ["grok-pattenrs"]
  ...
}
```

* Or pass the distributedcache option with grok dictionaries. Cover this example later.

* Grok Debugger: https://grokdebug.herokuapp.com 
 * Add custom patterns instead of `dirctionayStrings`
 * If don't know where to start, click 'Discover' menu, put the log into text box then click 'Discover' button. It will find some grok patterns for you.

* If custom grok debugger is needed, see https://github.com/nickethier/grokdebug
 * Ruby script.
```
rvm use ruby
bundle install
rackup config.ru
```

* See http://kitesdk.org/docs/current/kite-morphlines/morphlinesReferenceGuide.html#/grok