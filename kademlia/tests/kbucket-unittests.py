
"""
[TestClass]

public class KBucketTests

{

  [TestMethod, ExpectedException(typeof(TooManyContactsException))]

  public void TooManyContactsTest()

  {

    KBucket kbucket = new KBucket();

    // Add max # of contacts.

    Constants.K.ForEach(n => kbucket.AddContact(new Contact(null, new ID(n))));

    // Add one more.

    kbucket.AddContact(new Contact(null, new ID(21)));

  }

}

public static void ForEach(this int n, Action<int> action)

{

  for (int i = 0; i < n; i++)

  {

    action(i);

  }

}
"""
