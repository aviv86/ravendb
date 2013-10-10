﻿namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class Some
	{
		[Fact]
		public void T1()
		{
			var ids = ReadIds();

			using (var env = new StorageEnvironment(new PureMemoryPager()))
			{
				var rand = new Random();
				var testBuffer = new byte[79];
				rand.NextBytes(testBuffer);

				var trees = CreateTrees(env, 1, "tree");

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					foreach (var id in ids)
					{
						foreach (var tree in trees)
						{
							tree.Add(tx, id, new MemoryStream(testBuffer));
						}
					}

					tx.Commit();
				}

				ValidateRecords(env, trees, ids);
			}
		}

		private void ValidateRecords(StorageEnvironment env, IEnumerable<Tree> trees, IList<string> ids)
		{
			using (var snapshot = env.CreateSnapshot())
			{
				foreach (var tree in trees)
				{
					using (var iterator = snapshot.Iterate(tree.Name))
					{
						Assert.True(iterator.Seek(Slice.BeforeAllKeys));

						var keys = new HashSet<string>();

						var count = 0;
						do
						{
							keys.Add(iterator.CurrentKey.ToString());
							Assert.True(ids.Contains(iterator.CurrentKey.ToString()));
							Assert.NotNull(snapshot.Read(tree.Name, iterator.CurrentKey));

							count++;
						}
						while (iterator.MoveNext());

						Assert.Equal(ids.Count, tree.State.EntriesCount);
						Assert.Equal(ids.Count, count);
						Assert.Equal(ids.Count, keys.Count);
					}
				}
			}
		}


		private static IList<Tree> CreateTrees(StorageEnvironment env, int number, string prefix)
		{
			var results = new List<Tree>();

			using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (var i = 0; i < number; i++)
				{
					results.Add(env.CreateTree(tx, prefix + i));
				}

				tx.Commit();
			}

			return results;
		}

		private static IList<string> ReadIds()
		{
			using (var reader = new StreamReader("Bugs/Data/data.txt"))
			{
				string line;

				var results = new List<string>();

				while (!string.IsNullOrEmpty(line = reader.ReadLine()))
				{
					results.Add(line.Trim());
				}

				return results;
			}
		}
	}
}