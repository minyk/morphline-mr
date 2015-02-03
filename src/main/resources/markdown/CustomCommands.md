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
      quoteCharacter : "'"
      inputs :
      output : output
   }
}
```

* delimiter : delimiter of each columns
* quoteCharacter : quote char for formatting
* inputs : field names
* output : output field name

This command uses jcsv with shaded type.