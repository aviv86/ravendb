using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.MailingList
{
    public class Nick2 : RavenTestBase
    {
		// https://groups.google.com/forum/#!topic/ravendb/cMpYB6y277c
        
		[Fact]
        public void QueryWithSelectAfterSelectNewWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                new ProBamboosIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "users/1",
                        ProfileId = "usersProfile/1",
                        Location = new Location
                        {
                            Latitude = 24.2,
                            Longitude = 81.9
                        }
                    }, "users/1");
                    session.Store(new UserProfile
                    {
                        Id = "usersProfile/1",
                        MiniText = "mini text",
                        Title = "profile of users/1",
                        AvatarUrl = "http://user1.avatar.com"
                    }, "usersProfile/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, ProBamboosIndex>()
                        .Where(x => x.ProfileId != null)
                        .Select(user => new
                        {
                            Profile = RavenQuery.Load<UserProfile>(user.ProfileId),
                            User = user
                        })
                        .Select(model => new ProBamboo
                        {
                            ProfileId = model.Profile.Id,
                            UserId = model.User.Id,
                            Title = model.Profile.Title,
                            MiniText = model.Profile.MiniText,
                            Latitude = model.User.Location.Latitude,
                            Longitude = model.User.Location.Longitude,
                            AvatarUrl = model.Profile.AvatarUrl
                        });

                    var bamboos = query.ToList();

                }
            }
        }

        private class ProBamboo
        {
            public string ProfileId { get; set; }
            public string UserId { get; set; }
            public string Title { get; set; }
            public string MiniText { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            public string AvatarUrl { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string ProfileId { get; set; }
            public Location Location { get; set; }
        }

        private class Location
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }


        private class UserProfile
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string AvatarUrl { get; set; }
            public string MiniText { get; set; }
        }

        private class ProBamboosIndex : AbstractIndexCreationTask<User>
        {
            public ProBamboosIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Id,
                                   user.ProfileId,
                                   user.Location                                  
                               };
            }
        }
    }
}
