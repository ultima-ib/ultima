extern crate proc_macro; //comes with Rust
use proc_macro::TokenStream;
use quote::quote; //turns syn data struct back into code
use syn; //Rust code from string into a data structure

///this is called by #[derive(ClassName)] because of proc_macro_derive
#[proc_macro_derive(ClassName)] //matches Trait name
pub fn hello_macro_derive(input: TokenStream) -> TokenStream {
    // Construct a repr of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();
    //build trait impl 
    impl_class_name_macro(&ast)
}

fn impl_class_name_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl ClassName for #name { //#name is value of var name
            fn class_name(&self) -> String {
                //stringify converts expression into string without evaluating it, at compile time
                String::from(stringify!(#name))
            }
        }
    };
    gen.into() // convert quote into TokenStream
}
