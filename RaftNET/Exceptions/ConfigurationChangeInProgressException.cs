namespace RaftNET;

public class ConfigurationChangeInProgressException : Exception {
    public override string Message => "Configuration change in progress";
}