using System;
using System.Collections.Generic;
using System.Text;


public class picklistValues
{
    public bool active { get; set; }
    public bool defaultValue { get; set; }
    public string label { get; set; }
    public string validFor { get; set; }
    public string value { get; set; }
}
public class fields
{
    public string label { get; set; }
    public string name { get; set; }
    public int length { get; set; }
    public string type { get; set; }
    public bool updateable { get; set; }
    public Boolean calculated { get; set; }
    public int digits { get; set; }
    public int precision { get; set; }
    public int scale { get; set; }
    public bool idLookup { get; set; }
    public bool autoNumber { get; set; }
    public IList<picklistValues> pickListValues { get; set; }
    public List<string> referenceTo { get; set; }
}
public class ObjectDescription
{
    public IList<fields> fields { get; set; }
}