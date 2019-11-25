using System;
public class EntityDefinition
{
    public string QualifiedApiName { get; set; }
    public string DeveloperName { get; set; }
    public string DurableId { get; set; }
    public string KeyPrefix { get; set; }
    public string Label { get; set; }
    public Boolean IsFeedEnabled { get; set; }
    public Boolean IsQueryable { get; set; }
    public Boolean IsLayoutable { get; set; }
    public Boolean IsDeprecatedAndHidden { get; set; }
    public Boolean IsCustomSetting { get; set; }
    public Boolean IsCustomizable { get; set; }
    public Boolean IsSearchable { get; set; }
    public Boolean IsRetrieveable { get; set; }
    public string NamespacePrefix { get; set; }
}