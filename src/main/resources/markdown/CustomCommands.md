Morphline-MR Custom Commands
==============

# writeCSV

## import
```
    importCommands : [
      "com.github.minyk.morphlinesmr.commands.WriteCSVBuilder",
    ]
```

## options

```
{
   writeCSV {
      delimiter : ","
      inputs :
      output : output
   }
}
```

* delimiter : delimiter of each columns
* inputs : field names
* output : output field name

This command uses jcsv with shaded type.