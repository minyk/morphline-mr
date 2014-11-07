Dictionaries Of Grok
=====================

In this section, default grok dictionaries are explained.

# grok-patterns

This file contains full of basic grok patterns.

## Basic

Syntax | Regex pattern | Description 
-------|---------------|-------------
USERNAME | `[a-zA-Z0-9._-]+` | Alphanumeric User name.
USER     | `%{USERNAME}`     | Alias of `USERNAME`.
INT      | `(?:[+-]?(?:[0-9]+))` | Integer with +/- sign or not.
BASE10NUM | `(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\.[0-9]+)?)|(?:\.[0-9]+)))` | Base 10 number. 
NUMBER   | (?:%{BASE10NUM}) | Alias of `BASE10NUM`
BASE16NUM | `(?<![0-9A-Fa-f])(?:[+-]?(?:0x)?(?:[0-9A-Fa-f]+))` | Base 16 number.
BASE16FLOAT | `\b(?<![0-9A-Fa-f.])(?:[+-]?(?:0x)?(?:(?:[0-9A-Fa-f]+(?:\.[0-9A-Fa-f]*)?)|(?:\.[0-9A-Fa-f]+)))\b` | Base 16 float number

## More basic

Syntax | Regex pattern | Description 
-------|---------------|-------------
POSINT | `\b(?:[1-9][0-9]*)\b` | Positive integer.
NONNEGINT | `\b(?:[0-9]+)\b` | Integer without negative sign.
WORD | `\b\w+\b` | A word is assembled with a character from a-z, A-Z, 0-9, including the _ (underscore) character.
NOTSPACE | `\S+` | `\S` is the equivalent of `[^\s]`.
SPACE | `\s*` | Space
DATA | `.*?` | Any pattern in zero or once appeared.
GREEDYDATA | `.*` | Any pattern.
QUOTEDSTRING | ``` (?>(?<!\\)(?>"(?>\\.|[^\\"]+)+"|""|(?>'(?>\\.|[^\\']+)+')|''|(?>`(?>\\.|[^\\`]+)+`)|``)) ``` | Quoted string.
UUID | `[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}` | A universally unique identifier pattern.

## Networking

Syntax | Regex pattern | Description 
-------|---------------|-------------
MAC | `(?:%{CISCOMAC}|%{WINDOWSMAC}|%{COMMONMAC})` | MAC address pattern. This pattern include CISCO/WINDOWS/COMMON pattern.
CISCOMAC | `(?:(?:[A-Fa-f0-9]{4}\.){2}[A-Fa-f0-9]{4})` | MAC address pattern for Cisco H/W.
WINDOWSMAC | `(?:(?:[A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2})` | MAC pattern for Windows.
COMMONMAC | `(?:(?:[A-Fa-f0-9]{2}:){5}[A-Fa-f0-9]{2})` | MAC pattern in normal case.
IP  | `(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])` | IP address pattern.
HOSTNAME | `\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\.?|\b)` | Pattern for hostname. This pattern can be used for short or FQDN both.
HOST | `%{HOSTNAME}` | Alias of `HOSTNAME`
IPORHOST | `(?:%{HOSTNAME}|%{IP})` | Pattern for IP address of Hostname.

## Path

Syntax | Regex pattern | Description 
-------|---------------|-------------
PATH | `(?:%{UNIXPATH}|%{WINPATH})` | Can be used for Unix or Windows path detection.
UNIXPATH | `(?>/(?>[\w_%!$@:.,-]+|\\.)*)+` | Pattern for unix-like path.
TTY | `(?:/dev/(pts|tty([pq])?)(\w+)?/?(?:[0-9]+))` | Pattern for TTY path.
WINPATH | `(?>[A-Za-z]+:|\\)(?:\\[^\\?*]*)+` | Path pattern of Windows style.
URIPROTO | `[A-Za-z]+(\+[A-Za-z+]+)?` | Pattern for protocols in URI.
URIHOST | `%{IPORHOST}(?::%{POSINT:port})?` | Pattern for host or ip in the URI.
URIPATH | `(?:/[A-Za-z0-9$.+!*'(){},~:;=#%_\-]*)+` | Pattern for actual path in the URI. 
URIPARAM | `\?[A-Za-z0-9$.+!*'|(){},~#%&/=:;_?\-\[\]]*` | Pattern for parameters in the URI.
URIPATHPARAM | `%{URIPATH}(?:%{URIPARAM})?` | Path only or Parameter attached pattern for URI.
URI | `%{URIPROTO}://(?:%{USER}(?::[^@]*)?@)?(?:%{URIHOST})?(?:%{URIPATHPARAM})?` | The pattern for URI.

## Date

Syntax | Regex pattern | Description 
-------|---------------|-------------
MONTH | `\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b` | The pattern of month in alphabet.
MONTHNUM | `(?:0?[1-9]|1[0-2])` | Pattern for numeric month style.
MONTHDAY | `(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])` | Pattern of day in month.
DAY | `(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)` | Pattern for .
YEAR | `(?>\d\d){1,2}` | Pattern for year. Can be used for 4 digit or 2 digit both.

## Time

Syntax | Regex pattern | Description 
-------|---------------|-------------
HOUR | `(?:2[0123]|[01]?[0-9])` | Hour.
MINUTE | `(?:[0-5][0-9])` | Minute.
SECOND | `(?:(?:[0-5][0-9]|60)(?:[:.,][0-9]+)?)` | Second.
TIME | `(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])` | Combined pattern of hour, minute, second.

## Date expand

Syntax | Regex pattern | Description 
-------|---------------|-------------
DATE_US | `%{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}` | US style date pattern. mm-dd-yy or mm/dd/yy. 
DATE_EU | `%{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}` | EU style date pattern. dd.mm.yy, dd/mm/yy or dd-mm-yy.
ISO8601_TIMEZONE | `(?:Z|[+-]%{HOUR}(?::?%{MINUTE})?)` | Pattern for timezone in ISO8601.
ISO8601_SECOND | `(?:%{SECOND}|60)` | Pattern for second in ISO8601. `60` is valid value for this pattern.
TIMESTAMP_ISO8601 | `%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?` | ISO8601 style timestamp.
DATE | `%{DATE_US}|%{DATE_EU}` | Combined pattern of `DATE_US` and `DATE_EU`.
DATESTAMP | `%{DATE}[- ]%{TIME}` | Normal date-time style pattern.
TZ | `(?:[PMCE][SD]T)` | Timezone pattern in alphabet. Only includes US and EU timezone.
DATESTAMP_RFC822 | `%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{TIME} %{TZ}` | RFC 822 style datestamp.
DATESTAMP_OTHER | `%{DAY} %{MONTH} %{MONTHDAY} %{TIME} %{TZ} %{YEAR}` | Other pattern for datestamp.

## Syslog

Syntax | Regex pattern | Description 
-------|---------------|-------------
SYSLOGTIMESTAMP | `%{MONTH} +%{MONTHDAY} %{TIME}` | syslog style timestamp
PROG | `(?:[\w._/%-]+)` | Program name in syslog. 
SYSLOGPROG | `%{PROG:program}(?:\[%{POSINT:pid}\])?` | Pattern for name or pid.
SYSLOGHOST | `%{IPORHOST}` | Shorthand for `IPORHOST`
SYSLOGFACILITY | `<%{NONNEGINT:facility}.%{NONNEGINT:priority}>` | Syslog facility pattern.
HTTPDATE | `%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}` | Date pattern for http

## Shorthand

Syntax | Regex pattern | Description 
-------|---------------|-------------
QS | `%{QUOTEDSTRING}` | Alias of `QUOTEDSTRING`

## Log

Syntax | Regex pattern | Description 
-------|---------------|-------------
SYSLOGBASE | `%{SYSLOGTIMESTAMP:timestamp} (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:logsource} %{SYSLOGPROG}:` | Base pattern for syslog
COMBINEDAPACHELOG | `%{IPORHOST:clientip} %{USER:ident} %{USER:auth} \[%{HTTPDATE:timestamp}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})" %{NUMBER:response} (?:%{NUMBER:bytes}|-) %{QS:referrer} %{QS:agent}` | Pattern for basic httpd log.

## Log Levels
Syntax | Regex pattern | Description 
-------|---------------|-------------
LOGLEVEL | `([T|t]race|TRACE|[D|d]ebug|DEBUG|[N|n]otice|NOTICE|[I|i]nfo|INFO|[W|w]arn?(?:ing)?|WARN?(?:ING)?|[E|e]rr?(?:or)?|ERR?(?:OR)?|[C|c]rit?(?:ical)?|CRIT?(?:ICAL)?|[F|f]atal|FATAL|[S|s]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?)` | Log level pattern.

# java

This file contains Java related patterns.

Syntax | Regex pattern | Description 
-------|---------------|-------------
JAVACLASS | `(?:[a-zA-Z0-9-]+\.)+[A-Za-z0-9$]+` | Pattern for class of java.
JAVAFILE | `(?:[A-Za-z0-9_.-]+)` | Pattern for Java source file. 
JAVASTACKTRACEPART | `at %{JAVACLASS:class}\.%{WORD:method}\(%{JAVAFILE:file}:%{NUMBER:line}\)` | Pattern for stack trace message in the logs.

# linux-syslog

This file contains patterns for linux syslog.

## Syslog basic

Syntax | Regex pattern | Description 
-------|---------------|-------------
SYSLOGBASE2 | `(?:%{SYSLOGTIMESTAMP:timestamp}|%{TIMESTAMP_ISO8601:timestamp8601}) (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:logsource} %{SYSLOGPROG}:` | syslog pattern.
SYSLOGPAMSESSION | `%{SYSLOGBASE} (?=%{GREEDYDATA:message})%{WORD:pam_module}\(%{DATA:pam_caller}\): session %{WORD:pam_session_state} for user %{USERNAME:username}(?: by %{GREEDYDATA:pam_by})?` | Pattern includes syslog message fields.

## Cron job

Syntax | Regex pattern | Description 
-------|---------------|-------------
CRON_ACTION | `[A-Z ]+` | Cron action pattern.
CRONLOG | `%{SYSLOGBASE} \(%{USER:user}\) %{CRON_ACTION:action} \(%{DATA:message}\)` | Cron log pattern.

## Syslog line

Syntax | Regex pattern | Description 
-------|---------------|-------------
SYSLOGLINE | `%{SYSLOGBASE2} %{GREEDYDATA:message}` | syslog line pattern

## IETF 5424 syslog(8) format (see http://www.rfc-editor.org/info/rfc5424)

Syntax | Regex pattern | Description 
-------|---------------|-------------
SYSLOG5424PRI | `<%{NONNEGINT:syslog5424_pri}>` | Privilege pattern in syslog 5424.
SYSLOG5424SD | `\[%{DATA}\]+` | Data pattern in syslog 5424.
SYSLOG5424LINE | `%{SYSLOG5424PRI}%{NONNEGINT:syslog5424_ver} (?:%{TIMESTAMP_ISO8601:syslog5424_ts}|-) (?:%{HOSTNAME:syslog5424_host}|-) (?:%{WORD:syslog5424_app}|-) (?:%{WORD:syslog5424_proc}|-) (?:%{WORD:syslog5424_msgid}|-) (?:%{SYSLOG5424SD:syslog5424_sd}|-) %{GREEDYDATA:syslog5424_msg}` | Pattern for syslog 5424.

# postgresql

Default postgresql pg_log format pattern

Syntax | Regex pattern | Description 
-------|---------------|-------------
POSTGRESQL | `%{DATESTAMP:timestamp} %{TZ} %{DATA:user_id} %{GREEDYDATA:connection_id} %{POSINT:pid}` | Default log format.

# redis

Syntax | Regex pattern | Description 
-------|---------------|-------------
REDISTIMESTAMP | `%{MONTHDAY} %{MONTH} %{TIME}` | 
REDISLOG | `\[%{POSINT:pid}\] %{REDISTIMESTAMP:timestamp} \*` | 

# ruby

Syntax | Regex pattern | Description 
-------|---------------|-------------
RUBY_LOGLEVEL | `(?:DEBUG|FATAL|ERROR|WARN|INFO)` | 
RUBY_LOGGER | `[DFEWI], \[%{TIMESTAMP_ISO8601:timestamp} #%{POSINT:pid}\] *%{RUBY_LOGLEVEL:loglevel} -- +%{DATA:progname}: %{GREEDYDATA:message}` | ruby log pattern.

# Acknowledgements

nagios and mcollective patterns are covered later.