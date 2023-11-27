"""
[TestClass]

public class IDTests

{

  [TestMethod]

  public void LittleEndianTest()

  {

    byte[] test = new byte[20];

    test[0] = 1;

    Assert.IsTrue(new ID(test) == new BigInteger(1), "Expected value to be 1.");

  }

  [TestMethod]

  public void PositiveValueTest()

  { 

    byte[] test = new byte[20];

    test[19] = 0x80;

    Assert.IsTrue(new ID(test) == BigInteger.Pow(new BigInteger(2), 159), "Expected
     value to be 1.");

  }

  [TestMethod, ExpectedException(typeof(IDLengthException))]

  public void BadIDTest()

  {

    byte[] test = new byte[21];

    new ID(test);

  }

  [TestMethod]

  public void BigEndianTest()

  {

    byte[] test = new byte[20];

    test[19] = 0x80;

    Assert.IsTrue(new ID(test).AsBigEndianBool[0] == true, "Expected big endian bit
       15 to be set.");

    Assert.IsTrue(new ID(test).AsBigEndianBool[8] == false, "Expected big endian bit
       7 to NOT be set.");

  }

}
"""
