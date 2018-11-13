context("hooks")

test_that("hooks", {
  reg = makeTestRegistry()
  if (!is.null(reg$cluster.functions$hooks$pre.do.collection) || !is.null(reg$cluster.functions$hooks$post.sync))
    skip("Hooks already defined by Cluster Functions")
  reg$cluster.functions$hooks = insert(reg$cluster.functions$hooks, list(
    "pre.do.collection" = function(jc, ...) cat(jc$job.hash, "\n", sep = ""),
    "post.sync" = function(reg, ...) cat("post.syn", file = fs::path(reg$file.dir, "post.sync.txt"))
  ))

  jc = makeJobCollection(1, reg = reg)
  expect_function(jc$hooks$pre.do.collection, args = "jc")

  fn.ps = fs::path(reg$file.dir, "post.sync.txt")
  expect_false(fs::file_exists(fn.ps))

  batchMap(identity, 1, reg = reg)
  submitAndWait(reg, 1)
  syncRegistry(reg = reg)
  expect_true(fs::file_exists(fn.ps))

  lines = getLog(1, reg = reg)
  expect_true(reg$status[1]$job.hash %chin% lines)
})
