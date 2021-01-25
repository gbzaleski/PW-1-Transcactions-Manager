package cp1.solution;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

// Pomocznicza klasa pary do zapamietywania wykonanych operacji.
public class OperationPair
{
    private ResourceId rid;
    private ResourceOperation resOp;

    public OperationPair(ResourceId rid, ResourceOperation resOp)
    {
        this.rid = rid;
        this.resOp = resOp;
    }

    // Gettery jak w standardowej parze:
    public ResourceId getKey()
    {
        return rid;
    }
    public ResourceOperation getValue()
    {
        return resOp;
    }
}
