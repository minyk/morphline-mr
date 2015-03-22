Additional Commands

# WriteCSV

With this command, produce CSV value very easily.

The command provides the following configuration options:

- delimiter: The character concatenates any two fields. Must be a string of length one.
- inputs: Fields for concat.
- output: Field name for output.

Example Usage:
```
    importCommands : [
      "com.github.minyk.morphlinesmr.commands.WriteCSVBuilder",
    ]
...
      {
        writeCSV {
          delimiter : "|"
          output : output
          inputs : [ col3, col2, col1 ]
        }
      }
```

# subString

Substring given field with start, end indexes.

The command provides the following configuration options:

- field: Field name for substring.
- startAt: Start index
- endBefore: End index
- fill: This value is true and input value from field is shorter than end index, the result will be filled with fillChar. If false, shorter input value will cause command fail. Default is false.
- fillChar: Char for fill.

Examepl Usage:
```
    importCommands : [
      "com.github.minyk.morphlinesmr.commands.SubstringBuilder",
    ]
...
      {
        subString {
          field : col2
          startAt : 2
          endBefore : 6
          fill : true
          fillChar : " "
        }
      }
```