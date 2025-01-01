namespace FastRail.Server;

public class ErrorCodes {
    public static int Ok = 0;

    // System and server-side errors
    public static int SystemError = -1;
    public static int RuntimeInconsistency = -2;
    public static int DataInconsistency = -3;
    public static int ConnectionLoss = -4;
    public static int MarshallingError = -5;
    public static int Unimplemented = -6;
    public static int OperationTimeout = -7;
    public static int BadArguments = -8;
    public static int InvalidState = -9; // old client don't have

    // API errors
    public static int APIError = -100;
    public static int NoNode = -101; // *
    public static int NoAuth = -102;
    public static int BadVersion = -103; // *
    public static int NoChildrenForEphemerals = -108;
    public static int NodeExists = -110; // *
    public static int NotEmpty = -111;
    public static int SessionExpired = -112;
    public static int InvalidCallback = -113;
    public static int InvalidAcl = -114;
    public static int AuthFailed = -115;
    public static int Closing = -116;
    public static int Nothing = -117;

    public static int SessionMoved = -118;

    // Attempts to perform a reconfiguration operation when reconfiguration feature is disabled
    public static int ReconfigDisabled = -123;
}