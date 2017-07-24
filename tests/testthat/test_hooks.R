context("hooks")

test_that("hooks", {
  reg = makeRegistry(file.dir = NA, make.default = FALSE)
  if (!is.null(reg$cluster.functions$hooks$pre.do.collection) || !is.null(reg$cluster.functions$hooks$post.sync))
    skip("Hooks already defined by Cluster Functions")
  reg$cluster.functions$hooks = insert(reg$cluster.functions$hooks, list(
    "pre.do.collection" = function(jc, ...) cat(jc$job.hash, "\n", sep = ""),
    "post.sync" = function(reg, ...) cat("post.syn", file = fp(reg$file.dir, "post.sync.txt"))
  ))

  jc = makeJobCollection(1, reg = reg)
  expect_function(jc$hooks$pre.do.collection, args = "jc")

  fn.ps = fp(reg$file.dir, "post.sync.txt")
  expect_false(file.exists(fn.ps))

  batchMap(identity, 1, reg = reg)
  submitAndWait(reg, 1)

  syncRegistry(reg = reg)
  expect_true(file.exists(fn.ps))

  lines = getLog(1, reg = reg)
  expect_true(reg$status[1]$job.hash %chin% lines)
})
