using System;
using System.Threading;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Util
{
	public class EasyReaderWriterLock
	{
#if !SILVERLIGHT 
		readonly ReaderWriterLockSlim inner = new ReaderWriterLockSlim();

		public IDisposable EnterReadLock()
		{
			if(inner.IsReadLockHeld || inner.IsWriteLockHeld)
				return new DisposableAction(() => { });

			inner.EnterReadLock();
			return new DisposableAction(inner.ExitReadLock);
		}

		public IDisposable EnterWriteLock()
		{
			if (inner.IsWriteLockHeld)
				return new DisposableAction(() => { });

			inner.EnterWriteLock();
			return new DisposableAction(inner.ExitWriteLock);
		}
#else

#endif

	}
}