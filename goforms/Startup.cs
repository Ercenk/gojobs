using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(goforms.Startup))]
namespace goforms
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
