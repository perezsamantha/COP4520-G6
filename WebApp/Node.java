public class Node
{
  RemoteClient remoteClient;
  private int key;
  public Node(RemoteClient rc, int k)
  {
	remoteClient = rc;
    key = k;
  }

  public RemoteClient getValue()
  {
    return this.remoteClient;
  }

  public int getKey()
  {
    return this.key;
  }
}