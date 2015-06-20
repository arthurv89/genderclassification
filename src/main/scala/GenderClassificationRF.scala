class GenderClassificationRF {

//  // (U,[prob])
//  public PTable<String, String> classifyUsers(final PCollection<String> userIds) {
//    // (U,P)
//    final PTable<String, String> userToProduct = DataParser.userProduct();
//    // (P,C)
//    final PTable<String, String> productToCategory = DataParser.productCategory();
//
//    final PTable<String, String> userToNull = userIds.parallelDo(userToNullMapper,
//      DataTypes.STRING_TO_STRING_TABLE_TYPE);
//    final PTable<String, String> productToUser = new DefaultJoinStrategy<String, String, String>()
//      // (U,P) JOIN (U,U) = (U,(P,U))
//      .join(userToProduct, userToNull, JoinType.INNER_JOIN)
//      // (U,P)
//      .parallelDo(convertToUser_product, DataTypes.STRING_TO_STRING_TABLE_TYPE)
//      // (P,U)
//      .parallelDo(inverse, DataTypes.STRING_TO_STRING_TABLE_TYPE);
//
//    PGroupedTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
//      // (P,U) JOIN (P,C) = (P,(U,C))
//      .join(productToUser, productToCategory, JoinType.INNER_JOIN)
//      // (U,C)
//      .values()
//      // (U,C)
//      .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE)
//      // (U,[C])
//      .groupByKey();
//
//    return userToCategory
//      // (U,[G])
//      .mapValues(classify, DataTypes.STRING_TYPE);
}
